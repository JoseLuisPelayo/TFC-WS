package org.jpsoft.tfcws.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.jpsoft.tfcws.app.subscription.SubscriptionService;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.infra.memory.InMemoryPresence;
import org.jpsoft.tfcws.ws.codec.MsgCodec;
import org.jpsoft.tfcws.ws.msg.Envelope;
import org.jpsoft.tfcws.ws.msg.JoinPayload;
import org.jpsoft.tfcws.ws.msg.MsgType;
import org.jpsoft.tfcws.ws.msg.SubscribedPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Set;

/**
 * Flujo que se ejecuta cuando se establece una conexión WebSocket y se espera
 * un mensaje inicial de tipo "join" con la posición del cliente.
 *
 * <p>Responsabilidades principales:
 * <ul>
 *   <li>Leer el primer mensaje entrante del cliente (se asume que es un "join").</li>
 *   <li>Parsear ese mensaje para obtener una {@link Position} válida.</li>
 *   <li>Llamar a {@link SubscriptionService#suscribeInitialZones} para suscribir al cliente
 *       a las zonas (chunks) iniciales en función de su posición.</li>
 *   <li>Construir y devolver un {@link WebSocketMessage} JSON informando de las zonas suscritas
 *       (tipo de mensaje: {@code SUBSCRIBED}).</li>
 *   <li>En caso de error o timeout en el mensaje "join", usar una posición por defecto (0,0)
 *       y continuar con la suscripción.</li>
 * </ul>
 * <p>
 * Diseño y notas:
 * <ul>
 *   <li>Esta clase delega la lógica concreta de suscripción a {@link SubscriptionService}.</li>
 *   <li>Los métodos que parsean y construyen mensajes devuelven {@link Mono} para integrarse
 *       en cadenas reactivas sin bloquear.</li>
 *   <li>Hay un TODO para extraer los parsers a una clase separada si se desea mejorar la
 *       separación de responsabilidades.</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class OnConnectFlow {

    /**
     * Servicio encargado de gestionar las suscripciones a zonas.
     */
    private final SubscriptionService subscriptionService;
    /**
     * Gestor de presencia en memoria.
     */
    private final InMemoryPresence presence;
    /**
     * Codec para convertir objetos a mensajes WebSocket.
     */
    private final MsgCodec codec;

    /**
     * Mapper JSON usado para parsear y crear mensajes JSON de salida.
     */
    private final ObjectMapper objectMapper;

    private static final Logger log = LoggerFactory.getLogger(OnConnectFlow.class);

    /**
     * Tiempo máximo que se espera por el mensaje "join" inicial recibido desde el cliente.
     * Si no llega en este tiempo, se usa una posición por defecto (0,0).
     */
    private Duration joinTimeout = Duration.ofSeconds(5);

    /**
     * Ejecuta el flujo de conexión para una sesión WebSocket.
     *
     * <p>Comportamiento general:
     * <ol>
     *   <li>Obtiene el primer mensaje disponible del {@code inbound} usando {@code inbound.next()}.</li>
     *   <li>Aplica un timeout de {@link #joinTimeout}; si expira, se usa la posición por defecto.</li>
     *   <li>Convierte el payload de texto a {@link Position} con {@link #parseJoinErrorToPosition}.</li>
     *   <li>Si no hay mensaje (flux completado sin elementos) o si ocurre un error
     *       (timeout / parseo), se aplica un fallback a la posición (0,0).</li>
     *   <li>Con la posición resultante, construye el mensaje de suscripción y lo devuelve.
     *       El resultado es un {@link Mono}&lt;{@link WebSocketMessage}&gt; que puede ser enviado
     *       inmediatamente por el manejador WebSocket.</li>
     * </ol>
     *
     * <p>Notas sobre operadores usados en la cadena reactiva:
     * <ul>
     *   <li>{@code inbound.next()} transforma el flujo {@link Flux} en un {@link Mono}
     *       que emite sólo el primer elemento disponible o completa si no hay ninguno.</li>
     *   <li>{@code timeout(joinTimeout)} lanza un error si el primer elemento tarda más de
     *       {@code joinTimeout} en llegar; este error es manejado por {@code onErrorResume}.</li>
     *   <li>{@code map(...)} transforma el {@link WebSocketMessage} en su representación textual.</li>
     *   <li>{@code flatMap(this::parseJoinErrorToPosition)} encadena una operación asíncrona
     *       que puede devolver un error (por eso retorna {@link Mono}).</li>
     *   <li>{@code switchIfEmpty(...)} cubre el caso en el que el {@link Flux} entrante
     *       se completa sin emitir elementos (cliente no envió mensaje alguno).</li>
     *   <li>{@code onErrorResume(...)} captura errores (timeout, parseo inválido, etc.) y
     *       aplica un fallback controlado para continuar el flujo sin romper la conexión.
     * </ul>
     *
     * @param session objeto de sesión WebSocket (usado para obtener id y dirección)
     * @param inbound flujo de mensajes entrantes desde el cliente
     * @return un {@link Mono} que emite un {@link WebSocketMessage} con la confirmación de suscripción
     */
    public Flux<WebSocketMessage> run(WebSocketSession session, Flux<Envelope> bus) {
        final String sessionId = session.getId();
        final InetSocketAddress remoteAddress = session.getHandshakeInfo().getRemoteAddress();

        // Primero: obtener el primer mensaje del cliente (si existe). Si tarda más de joinTimeout,
        // se considera timeout y se cae al onErrorResume.
        Mono<Position> positionMono = bus
                .filter(envelope -> envelope.getType() == MsgType.JOIN)
                .next()
                .timeout(joinTimeout)
                .flatMap(
                        envelope -> {
                            try {
                                JoinPayload joinPayload = codec.parsePayload(envelope, JoinPayload.class);
                                return Mono.just(new Position(joinPayload.getX(), joinPayload.getY()));
                            } catch (JsonProcessingException e) {
                                return Mono.error(e);
                            }
                        })
                .switchIfEmpty(
                        Mono.defer(() -> {
                            log.error("Join_no_message_received -> fallback_position to (0,0) for sessionId={}, remoteAddress={}",
                                    sessionId, remoteAddress);
                            return Mono.just(new Position(0.0, 0.0));
                        })
                )
                .onErrorResume(ex -> {
                    log.error("Join_timeout_or_invalid_message -> fallback_position to (0,0) for sessionId={}, remoteAddress={} cause: {}",
                            sessionId, remoteAddress, ex.getMessage());
                    return Mono.just(new Position(0.0, 0.0));
                });

        // Suscribir a las zonas iniciales basadas en la posición obtenida
        Mono<Set<ChunkCoord>> zonesMono = positionMono.map(pos -> subscriptionService.suscribeInitialZones(sessionId, pos));

        // Construir el flujo de snapshots para las zonas suscritas
        Mono<WebSocketMessage> suscribedSnapshots = zonesMono
                .map(zones ->
                        codec.encode(MsgType.SUBSCRIBED, new SubscribedPayload(zones)))
                .map(session::textMessage);

        Flux<WebSocketMessage> snapshots = zonesMono.flatMapMany(
                        zones -> Flux.fromIterable(presence.buildSnapShotZone(sessionId, zones)))
                .map(snapShotZonePayload -> session.textMessage(codec.encode(MsgType.SNAPSHOT_ZONE, snapShotZonePayload)));

        return suscribedSnapshots.concatWith(snapshots);

    }

}

