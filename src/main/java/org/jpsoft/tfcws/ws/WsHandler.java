package org.jpsoft.tfcws.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jpsoft.tfcws.app.subscription.SessionRegistry;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.domain.world.ChunkGeometry;
import org.jpsoft.tfcws.infra.memory.InMemoryPresence;
import org.jpsoft.tfcws.ws.codec.MsgCodec;
import org.jpsoft.tfcws.ws.msg.Envelope;
import org.jpsoft.tfcws.ws.msg.PlayerMovePayload;
import org.jpsoft.tfcws.ws.msg.PlayerMovedAckPayload;
import org.jpsoft.tfcws.ws.msg.error.ErrorCode;
import org.jpsoft.tfcws.ws.msg.error.ErrorPayload;
import org.jpsoft.tfcws.ws.msg.MsgType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;

/**
 * Controlador WebSocket reactivo basado en Spring WebFlux.
 *
 * <p>Responsabilidades principales:
 * <ul>
 *   <li>Abrir y gestionar el ciclo de vida de una sesión WebSocket.</li>
 *   <li>Crear los streams reactivos de entrada (inbound) y salida (outbound).</li>
 *   <li>Inyectar un mensaje inicial producido por {@link OnConnectFlow} antes de mantener
 *       un latido (heartbeat) periódico.</li>
 *   <li>Registrar la finalización de sesión y eliminar la sesión del registro mediante
 *       {@link SessionRegistry} en la finalización.</li>
 * </ul>
 *
 * <p>Diseño y garantías:
 * <ul>
 *   <li>No almacena mensajes de forma explícita en memoria: delega en el backpressure de WebFlux.</li>
 *   <li>Comparte la fuente de mensajes entrantes con {@code publish().autoConnect(2)}
 *       para que tanto la lógica de conexión como la terminación puedan consumirla.</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WsHandler implements WebSocketHandler {

    /**
     * Registro de sesiones: se usa para limpiar/gestionar suscripciones y datos asociados
     * cuando la sesión termina.
     */
    private final SessionRegistry sessionRegistry;
    /**
     * Gestor de presencia en memoria: puede ser usado por los flujos para actualizar
     * la presencia de sesiones/jugadores.
     */
    private final InMemoryPresence presence;
    /**
     * Flujo que se ejecuta al establecer la conexión y que puede producir un mensaje
     * inicial (p. ej. validación, saludo, suscripciones iniciales).
     *
     * <p>Se invoca con la sesión y el stream de entrada (inbound) para permitir
     * que la lógica de conexión lea mensajes iniciales del cliente si es necesario.
     */
    private final OnConnectFlow connectFlow;

    private final MsgCodec codec;

    /**
     * Maneja una conexión WebSocket.
     *
     * <p>Construye y enlaza dos pipelines principales:
     * <ul>
     *   <li><b>inbound</b>: flujo de texto recibido desde el cliente ({@code session.receive()}).</li>
     *   <li><b>outbound</b>: mensajes enviados al cliente; aquí se concatena
     *       el mensaje producido por {@link OnConnectFlow} y un {@code heartbeat} periódico.</li>
     * </ul>
     *
     * <p>Flujo de ejecución relevante:
     * <ol>
     *   <li>Se obtiene un {@code inbound} compartido usando {@code publish().autoConnect(2)}:
     *       esto permite que {@code connectFlow} y la finalización del {@code inbound} consuman la
     *       misma fuente sin re-suscribirse múltiples veces.</li>
     *   <li>Se solicita a {@code connectFlow.run(session, inbound)} un {@code Mono<WebSocketMessage>}
     *       que se envía primero al cliente si está presente.</li>
     *   <li>Se crea un {@code heartbeat} periódico (ping cada 30s) para mantener viva la conexión.</li>
     *   <li>Se concatena el mensaje de conexión y el heartbeat para formar {@code outbound}.</li>
     *   <li>Se envía {@code outbound} al cliente y se espera la finalización del {@code inbound}.</li>
     *   <li>En {@code doFinally} se limpia la sesión en {@link SessionRegistry} y se registra el cierre.</li>
     * </ol>
     *
     * <p>Nota sobre garantías: el {@code Mono<Void>} devuelto completa cuando finaliza el envío
     * de {@code outbound} (cierre remoto, error o cancelación) y cuando {@code inbound} termina.
     *
     * @param session la sesión WebSocket activa
     * @return un {@code Mono<Void>} que representa la finalización del manejo de la sesión
     */
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        String id = session.getId();

        String address = String.valueOf(session.getHandshakeInfo().getRemoteAddress());

        // Inbound: convertimos cada mensaje WebSocket a texto y lo compartimos para múltiples consumidores.
        Flux<String> inboundText = session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .share();

        /*
         * Bus: parseamos cada texto a Envelope. En caso de tener JSON inválido generamos
         * un envelope de error para que la capa superior pueda notificar al cliente.
         * Compartimos el stream para reutilizar la misma fuente en distintos subsistemas.
         */
        Flux<Envelope> bus = inboundText
                .flatMap(text -> Mono.fromCallable(() -> codec.parseEnvelope(text))
                        .onErrorResume(e -> {
                            try {
                                return Mono.just(
                                        codec.parseEnvelope(
                                                codec.encode(
                                                MsgType.ERROR,
                                                new ErrorPayload(ErrorCode.BAD_JSON.name(), ErrorCode.BAD_JSON.defaultMessage())
                                        )));
                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }
                        }))
                .share();

        // Ejecuta la lógica de conexión que puede producir un mensaje inicial (o vaciarse).
        Flux<WebSocketMessage> connectMessage = connectFlow.run(session, bus);

        /*
         * Movimiento: cada Envelope de tipo MOVE se transforma en un ack enviado al cliente.
         * Si la sesión no pertenece a ninguna zona se responde con un error NOT_JOINED.
         * También usamos esta ruta para actualizar la presencia antes de enviar el echo.
         */
        Flux<WebSocketMessage> movementEcho = bus
                .filter(env -> env.getType() == MsgType.MOVE)
                .flatMap(
                        env -> {
                            try {
                                if (sessionRegistry.getZonesBySessionId(id).isEmpty()) {
                                    // La sesión no ha entrado en ninguna zona, mandamos error.
                                    return Mono.just(session.textMessage(
                                            codec.encode(MsgType.ERROR,
                                                    new ErrorPayload(ErrorCode.NOT_JOINED.name(), ErrorCode.NOT_JOINED.defaultMessage()))
                                    ));
                                }

                                PlayerMovePayload move = codec.parsePayload(env, PlayerMovePayload.class);
                                presence.upsertPresence(id, new Position(move.getX(), move.getY()), sessionRegistry.getZonesBySessionId(id));
                                // Echo del movimiento: convertimos la posición a chunk y devolvemos el ack.
                                ChunkCoord chunkCoord = ChunkGeometry.posToChunk(new Position(move.getX(), move.getY()));

                                return Mono.just(session.textMessage(
                                        codec.encode(MsgType.MOVED,
                                                new PlayerMovedAckPayload(move.getX(), move.getY(), chunkCoord.getZoneKey())))
                                );


                            } catch (JsonProcessingException e) {
                                // Falla el parseo del payload MOVE, respondemos con BAD_MOVE.
                                String json = codec.encode(MsgType.ERROR, new ErrorPayload("BAD_MOVE", e.getMessage()));
                                return Mono.just(session.textMessage(json));
                            }
                        }
                );

        // Latido periódico: genera mensajes de tipo ping cada 30 segundos para mantener la conexión.
        Flux<WebSocketMessage> heartbeat = Flux.interval(Duration.ofSeconds(30))
                .map(tick -> session.pingMessage(buf -> buf.wrap(new byte[0])));

        // Outbound: concatenamos el mensaje de conexión inicial, los echos y los pings de heartbeat.
        Flux<WebSocketMessage> outbound = Flux.concat(connectMessage, movementEcho, heartbeat);



        // Envía el outbound y espera la finalización del inbound.
        // En doFinally se realiza la limpieza del registro de sesiones y el log.
        return session.send(outbound)
                .and(inboundText.then())
                .doFinally(s -> {

                    String sessionId = session.getId();
                    Set<ChunkCoord> zones = sessionRegistry.getZonesBySessionId(sessionId);
                    if (!zones.isEmpty()) {
                        presence.removePresence(sessionId, zones);
                    }
                    sessionRegistry.removeSession(session.getId());

                    log.info("WebSocket session ended: id={}, address={}", id, address);
                });

    }
}
