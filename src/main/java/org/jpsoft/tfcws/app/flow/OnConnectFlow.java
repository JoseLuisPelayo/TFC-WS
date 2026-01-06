package org.jpsoft.tfcws.app.flow;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import org.jpsoft.tfcws.adapter.ws.msg.*;
import org.jpsoft.tfcws.app.port.Presence;
import org.jpsoft.tfcws.app.port.SessionRegistry;
import org.jpsoft.tfcws.app.port.SessionStateStore;
import org.jpsoft.tfcws.app.port.WsMessenger;
import org.jpsoft.tfcws.domain.actor.Direction;
import org.jpsoft.tfcws.domain.actor.EntityState;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.domain.world.ChunkGeometry;
import org.jpsoft.tfcws.adapter.ws.MsgCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;

/**
 * OnConnectFlow
 * <p>
 * Responsable de orquestar la conexión inicial de un cliente WebSocket conectado al servidor.
 * Esta clase transforma el flujo de mensajes entrantes (bus) en los mensajes que se deben enviar
 * inicialmente al cliente: primero un mensaje SUBSCRIBED con las zonas asignadas y luego una secuencia
 * de SNAPSHOT_ZONE que describen el estado de cada zona.
 * <p>
 * Comportamiento y efectos secundarios:
 * - Espera el primer mensaje de tipo JOIN enviado por el cliente y extrae la posición.
 * - Si el JOIN no llega o es inválido, aplica un fallback a la posición (0,0) y continúa.
 * - Calcula el chunk y las zonas AOI (area of interest) a partir de la posición.
 * - Registra la sesión en el `SessionRegistry` y la presencia en el `Presence`.
 * - Guarda el estado inicial de la sesión en `SessionStateStore`.
 * - Construye y devuelve un flujo de WebSocketMessage que primero emite SUBSCRIBED y después
 * los SNAPSHOT_ZONE para cada zona (obtenidos de `Presence.buildSnapShotZone`).
 * <p>
 * Contrato (inputs/outputs):
 * - Entrada: `WebSocketSession` (meta de sesión) y `Flux<Envelope>` (mensajes entrantes desde cliente).
 * - Salida: `Flux<WebSocketMessage>` (mensajes que deben ser enviados al cliente inmediatamente tras conectar).
 * <p>
 * Mecanismo de error y tolerancia:
 * - Se aplica un timeout configurable (JOIN_TIMEOUT). Si el mensaje JOIN no llega a tiempo
 * o no puede parsearse, se hace fallback a la posición (0,0) y el proceso continúa.
 * - Los errores derivados del parseo del payload se manejan con logs y fallback; no se propagan
 * hacia el cliente en esta etapa.
 * <p>
 * Notas de diseño:
 * - Basado en Reactor (Flux/Mono) para no bloquear el hilo que atiende la conexión.
 * - Las operaciones que realizan efectos secundarios (registro de sesión, presencia, store)
 * se ejecutan en la rama reactiva tras resolverse la posición inicial.
 * - La construcción de mensajes JSON/text se delega a {@link MsgCodec}.
 * <p>
 */
@Component
@RequiredArgsConstructor
public class OnConnectFlow {
    /**
     * Gestor de presencia en memoria.
     */
    private final Presence presence;
    /**
     * Registro de sesiones por zona.
     */
    private final SessionRegistry sessionRegistry;
    /**
     * Almacenamiento del estado de la sesión.
     */
    private final SessionStateStore sessionStateStore;
    /**
     * Codec para convertir objetos a mensajes WebSocket.
     */
    private final MsgCodec codec;
    /**
     * Mensajería WebSocket para enviar mensajes al cliente.
     */
    private final WsMessenger wsMessenger;

    private static final Logger log = LoggerFactory.getLogger(OnConnectFlow.class);
    /**
     * Tiempo máximo que se espera por el mensaje "join" inicial recibido desde el cliente.
     * Si no llega en este tiempo, se usa una posición por defecto (0,0).
     */
    private final Duration JOIN_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Ejecuta el flujo de conexión para una sesión WebSocket.
     * <p>
     * Flujo detallado:
     * 1) Filtrar el `bus` para obtener el primer `Envelope` de tipo JOIN.
     * 2) Aplicar un timeout (JOIN_TIMEOUT) para evitar bloqueos indefinidos.
     * 3) Intentar parsear el payload a `JoinPayload` y obtener la posición (x,y).
     * - Si el parseo falla o hay un error en cualquier punto, se registra el error y
     * se aplica una posición por defecto (0,0) para continuar de forma tolerante.
     * 4) A partir de la posición: calcular el `ChunkCoord` y las zonas AOI (set de chunks).
     * 5) Registrar efectos secundarios:
     * - `sessionRegistry.addSessionsToZones(sessionId, zones)`
     * - `presence.upsertPresence(sessionId, pos, zones)`
     * - `sessionStateStore.upsert(...)`
     * 6) Construir y devolver un `Flux<WebSocketMessage>` que contiene:
     * - Un mensaje `SUBSCRIBED` con las zonas asignadas.
     * - Una concatenación de mensajes `SNAPSHOT_ZONE` (uno por payload devuelto por presence).
     * <p>
     * Detalle de operadores Reactor empleados (qué hacen):
     * - filter(...).next(): busca el primer `Envelope` de tipo JOIN y lo convierte en un Mono.
     * - timeout(...): si no se recibe a tiempo, produce un error que se captura y transforma en fallback.
     * - flatMap(...): parsea el envelope y devuelve un Mono<Position>; errores de parseo se convierten en Mono.error.
     * - switchIfEmpty(...): si no hubo ningún JOIN en el flujo, aplica un fallback con posición (0,0).
     * - onErrorResume(...): captura errores (timeout, parseo, otros) y aplica fallback con posición (0,0).
     * - flatMapMany(pos -> ...): a partir de la posición resultante, genera un flujo (Flux) con los mensajes
     * que deben enviarse al cliente (SUBSCRIBED + SNAPSHOT_ZONE...).
     *
     * @param session objeto de sesión WebSocket (usado para obtener id y dirección)
     * @param bus     flujo de mensajes entrantes desde el cliente
     * @return un {@link Flux} que primero emite el mensaje de suscripción y luego los snapshots asociados
     */
    public Mono<Void> run(WebSocketSession session, Flux<Envelope> bus) {
        final String sessionId = session.getId();
        final InetSocketAddress remoteAddress = session.getHandshakeInfo().getRemoteAddress();

        // -----------------------------
        // 1) Obtener posición inicial
        // -----------------------------
        // Comentarios inline sobre el pipeline reactivo:
        // - filter(envelope -> envelope.getType() == MsgType.JOIN): dejamos solo mensajes JOIN
        // - next(): transformamos el Flux en Mono que emite solamente la primera coincidencia
        // - timeout(JOIN_TIMEOUT): si no llega, Reactor lanza un TimeoutException
        // - flatMap(... parseo ...): parseamos el payload JSON a JoinPayload; si falla, devolvemos Mono.error
        // - switchIfEmpty(...): si no hubo ningún JOIN, aplicamos fallback (0,0)
        // - onErrorResume(...): capturamos errores (timeout, parseo) y también aplicamos fallback (0,0)

        return bus
                .filter(envelope -> envelope.getType() == MsgType.JOIN)
                .next()
                .timeout(JOIN_TIMEOUT)
                .flatMap(
                        envelope -> {
                            try {
                                JoinPayload joinPayload = codec.parsePayload(envelope, JoinPayload.class);
                                return Mono.just(new Position(joinPayload.getX(), joinPayload.getY()));
                            } catch (JsonProcessingException e) {
                                // Si el payload no es JSON válido para JoinPayload, propagamos error
                                return Mono.error(e);
                            }
                        })
                .switchIfEmpty(
                        Mono.defer(() -> {
                            // No hubo JOIN en el flujo: fallback a (0,0)
                            log.error("Join_no_message_received -> fallback_position to (0,0) for sessionId={}, remoteAddress={}",
                                    sessionId, remoteAddress);
                            return Mono.just(new Position(0.0, 0.0));
                        })
                )
                .onErrorResume(ex -> {
                    // Timeout o parseo inválido: registramos y devolvemos posición por defecto
                    log.error("Join_timeout_or_invalid_message -> fallback_position to (0,0) for sessionId={}, remoteAddress={} cause: {}",
                            sessionId, remoteAddress, ex.getMessage());
                    return Mono.just(new Position(0.0, 0.0));
                }).flatMap(pos -> {

                    // Calcular el chunk que contiene la posición y las zonas de AOI (necesarias para suscripción)
                    ChunkCoord chunkCoord = ChunkGeometry.posToChunk(pos);
                    Set<ChunkCoord> zones = ChunkGeometry.getChunksInAOI(chunkCoord);

                    EntityState state = new EntityState(
                            sessionId,
                            zones,
                            pos,
                            chunkCoord,
                            Direction.SOUTH,
                            System.currentTimeMillis(),
                            System.currentTimeMillis());

                    // Guardar estado inicial de la sesión
                    sessionStateStore.upsert(sessionId, state);

                    // Efectos secundarios: registrar la sesión en el registry y la presencia del jugador
                    sessionRegistry.addSessionsToZones(sessionId, zones);
                    presence.upsertPresence(sessionId, pos);

                    log.info("Session_joined -> sessionId={}, remoteAddress={}, position=({},{}), chunk=({},{}), zones={}",
                            sessionId, remoteAddress, pos.x(), pos.y(), chunkCoord.cx(), chunkCoord.cy(), zones);

                    // Notificar al jugador que su estado inicial está listo
                    wsMessenger.sendTo(sessionId, MsgType.INITIAL_STATE, new PlayerViewPayload(
                            sessionId,
                            "nombre_jugador",
                            state.currentPosition().x(),
                            state.currentPosition().y(),
                            state.direction()
                    ));

                    // Notificar a otros jugadores en la misma zona que este jugador ha cargado
                    wsMessenger.broadcastToZone(chunkCoord, MsgType.PLAYER_LOADED, new PlayerViewPayload(
                            sessionId,
                            "nombre_jugador",
                            state.currentPosition().x(),
                            state.currentPosition().y(),
                            state.direction()));

                    // Construir el mensaje SUBSCRIBED (respuesta inmediata al cliente)
                    wsMessenger.sendTo(sessionId, MsgType.SUBSCRIBED, new SubscribedPayload(zones));

                    log.info("connect_snapshots_start sessionId={} zones={}", sessionId, zones.size());
                    // Enviar snapshot de las zonas asignadas
                    zones.forEach(zone -> {
                        Set<String> entitiesInZone = presence.getEntitiesInZone(zone);

                        var players = entitiesInZone.stream()
                                .filter(entityId -> !entityId.equals(sessionId))
                                .map(sessionStateStore::get)
                                .flatMap(Optional::stream)
                                .map(pState -> new PlayerViewPayload(
                                        pState.playerId(),
                                        "nombre_jugador",
                                        pState.currentPosition().x(),
                                        pState.currentPosition().y(),
                                        pState.direction()
                                ))
                                .toList();

                        if (!players.isEmpty()) {
                            wsMessenger.sendTo(sessionId, MsgType.SNAPSHOT_ZONE,
                                    new SnapShotZonePayload(zone.getZoneKey(), players));
                        }
                    });

                    /*zones.forEach(zone -> {
                        Set<String> entitiesInZone = presence.getEntitiesInZone(zone);
                        if (entitiesInZone.isEmpty()) {
                            log.info("Snapshot_zone_empty -> sessionId={}, zone={}", sessionId, zone.getZoneKey());
                        }
                        log.info("connect_snapshot_zone sessionId={} zone={} entities={}",
                                sessionId, zone.getZoneKey(), entitiesInZone);

                        wsMessenger.sendTo(sessionId, MsgType.SNAPSHOT_ZONE, new SnapShotZonePayload(zone.getZoneKey(), entitiesInZone.stream()
                                .filter(entityId -> !entityId.equals(sessionId))
                                .map(sessionStateStore::get)
                                .flatMap(Optional::stream)
                                .map(pState -> new PlayerViewPayload(
                                        pState.playerId(),
                                        "nombre_jugador",
                                        pState.currentPosition().x(),
                                        pState.currentPosition().y(),
                                        pState.direction()
                                ))
                                .toList()));
                    });*/

                    return Mono.empty();
                })
                .then();
    }

}
