package org.jpsoft.tfcws.app.flow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.jpsoft.tfcws.app.port.Presence;
import org.jpsoft.tfcws.app.subscription.SubscriptionService;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.infra.memory.InMemoryPresence;
import org.jpsoft.tfcws.adapter.ws.MsgCodec;
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
 * Flujo que prepara la conexión inicial de un cliente WebSocket.
 *
 * <p>Responsabilidades claves:
 * <ul>
 *   <li>Escuchar el primer {@code Envelope} de tipo {@code JOIN} que envía el cliente.</li>
 *   <li>Aplicar un timeout y fallback (posición "0,0") cuando el mensaje no llega o está mal formado.</li>
 *   <li>Delegar en {@link SubscriptionService#suscribeInitialZones} la suscripción a los chunks iniciales.</li>
 *   <li>Generar el mensaje {@code SUBSCRIBED} con los chunks asignados y emitir los correspondientes
 *       mensajes {@code SNAPSHOT_ZONE} que provienen de {@link InMemoryPresence#buildSnapShotZone}.</li>
 * </ul>
 *
 * <p>Diseño:
 * <ul>
 *   <li>El flujo se integra con Reactor para no bloquear la lectura del mensaje inicial.</li>
 *   <li>Se registran errores y se aplica un fallback implícito (posición 0,0) antes de continuar.</li>
 *   <li>Las cargas útiles JSON las construye {@link MsgCodec} y las convierte en {@link WebSocketMessage}.</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class OnConnectFlow {

    /**
     * Servicio encargado de gestionar las suscripciones a zonas.
     */
    private final SubscriptionService subscribedSnapshots;
    /**
     * Gestor de presencia en memoria.
     */
    private final Presence presence;
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
    private final Duration joinTimeout = Duration.ofSeconds(5);

    /**
     * Ejecuta el flujo de conexión para una sesión WebSocket.
     *
     * <p>Este metodo sigue los pasos:
     * <ol>
     *   <li>Filtra el stream {@code bus} para quedarse con el primer envelope {@code JOIN}.</li>
     *   <li>Aplica un timeout de {@link #joinTimeout}; si expira o el payload es inválido, se resuelve con la
     *       posición por defecto (0,0) registrando el error.</li>
     *   <li>Invoca al {@link SubscriptionService} para subscribir al jugador a las zonas iniciales
     *       en función de la posición obtenida.</li>
     *   <li>Construye un mensaje {@code SUBSCRIBED} y una secuencia de {@code SNAPSHOT_ZONE}
     *       usando los datos devueltos por {@link InMemoryPresence#buildSnapShotZone}.
     * </ol>
     *
     * <p>Notas sobre la implementación reactiva:
     * <ul>
     *   <li>{@code next()} convierte el {@link Flux} en un {@link Mono} que emite solo la primera coincidencia.</li>
     *   <li>{@code timeout(...)} asegura que no se bloquea indefinidamente esperando el mensaje.</li>
     *   <li>{@code switchIfEmpty(...)} y {@code onErrorResume(...)} aplican la posición de respaldo.
     *   <li>Las etapas posteriores transforman los datos en mensajes Texto que se envían al cliente.</li>
     * </ul>
     *
     * @param session objeto de sesión WebSocket (usado para obtener id y dirección)
     * @param bus flujo de mensajes entrantes desde el cliente
     * @return un {@link Flux} que primero emite el mensaje de suscripción y luego los snapshots asociados
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
        Mono<Set<ChunkCoord>> zonesMono = positionMono.map(pos -> subscribedSnapshots.suscribeInitialZones(sessionId, pos));

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
