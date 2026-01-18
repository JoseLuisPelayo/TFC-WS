package org.jpsoft.tfcws.app.flow;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import org.jpsoft.tfcws.adapter.ws.msg.*;
import org.jpsoft.tfcws.adapter.ws.msg.error.ErrorPayload;
import org.jpsoft.tfcws.app.port.*;
import org.jpsoft.tfcws.app.service.PlayerService;
import org.jpsoft.tfcws.domain.actor.Direction;
import org.jpsoft.tfcws.domain.actor.EntityState;
import org.jpsoft.tfcws.domain.actor.SessionState;
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
import java.util.UUID;

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
    private final EntityStateStore entityStateStore;
    /**
     * Almacenamiento de identidad de sesión (sessionId -> userId).
     */
    private final SessionStateStore sessionStateStore;
    /**
     * Servicio de gestión de personajes.
     */
    private final PlayerService playerService;
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
    private final Duration JOIN_TIMEOUT = Duration.ofSeconds(300);

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

        /*De momento usamos el fallback y ponemos los personajes en 0.0 cuando tengamos base de
        datos la posicion inicial vendra de base e datos*/
        return bus
                .filter(envelope -> envelope.getType() == MsgType.JOIN)
                .next()
                .timeout(JOIN_TIMEOUT)
                .flatMap(
                        envelope -> {
                            JoinPayload joinPayload;

                            Optional<SessionState> sessionState = sessionStateStore.getSessionState(sessionId);

                            UUID userId = sessionState.map(SessionState::getUserId).orElse(null);

                            if (userId == null) {
                                // La sesión no está autenticada; no permitimos continuar con el flujo de conexión.
                                log.warn("Unauthenticated_session -> sessionId={}, remoteAddress={}", sessionId, remoteAddress);
                                wsMessenger.sendTo(sessionId, MsgType.ERROR, new ErrorPayload("UNAUTHENTICATED", "Session is not authenticated"));
                                return Mono.empty();
                            }

                            try {
                                joinPayload = codec.parsePayload(envelope, JoinPayload.class);
                                String playerName = joinPayload.getPlayerName();

                                return playerService.getOrCreatePlayer(userId, playerName)
                                        .flatMap(player -> {
                                            //Bloqueamos aqui porque necesitamos el player para continuar ;
                                            //Comprobar que no es null
                                            // aqui tengo el player
                                            // si no tiene ultima posicion pues 0,0
                                            log.info("Player_lookup -> userId={}, playerName={}, playerPos=({},{})",
                                                    userId, playerName,
                                                    player.getLastXPosition(),
                                                    player.getLastYPosition()
                                            );


                                            Position pos = new Position(
                                                    player.getLastXPosition(),
                                                    player.getLastYPosition()
                                            );

                                            log.info("Player_found -> playerId={}, playerName={}, position=({},{})",
                                                    player.getId(), playerName, pos.x(), pos.y());

                                            ChunkCoord chunkCoord = ChunkGeometry.posToChunk(pos);
                                            Set<ChunkCoord> zones = ChunkGeometry.getChunksInAOI(chunkCoord);
                                            // guardar estado de la sesion con la posicion del player

                                            EntityState state = new EntityState(
                                                    player.getId(),
                                                    zones,
                                                    pos,
                                                    chunkCoord,
                                                    Direction.SOUTH,
                                                    System.currentTimeMillis(),
                                                    System.currentTimeMillis());

                                            // Guardar estado inicial de la sesión
                                            entityStateStore.upsert(player.getId(), state);
                                            // guardar presencia con la posicion del player
                                            presence.upsertPresence(player.getId(), pos);
                                            // guardar en session registry
                                            sessionRegistry.addSessionsToZones(sessionId, zones);
                                            sessionStateStore.bind(sessionId,
                                                    SessionState.builder()
                                                            .userId(userId)
                                                            .playerId(player.getId())
                                                            .build()
                                            );

                                            log.info("Session_joined -> sessionId={}, remoteAddress={}, position=({},{}), chunk=({},{}), zones={}",
                                                    sessionId, remoteAddress, pos.x(), pos.y(), chunkCoord.cx(), chunkCoord.cy(), zones);
                                            // tengo que enviar el initial state con el id del player y su ultima posicion
                                            // Notificar al jugador que su estado inicial está listo
                                            wsMessenger.sendTo(sessionId, MsgType.INITIAL_STATE, new PlayerViewPayload(
                                                    player.getId(),
                                                    "nombre_jugador",
                                                    state.currentPosition().x(),
                                                    state.currentPosition().y(),
                                                    state.direction()
                                            ));

                                            // enviar snapshot zone a los demas
                                            // Notificar a otros jugadores en la misma zona que este jugador ha cargado
                                            //TODO buscar la manera de filtrar a self
                                            wsMessenger.broadcastToZone(chunkCoord, MsgType.PLAYER_LOADED, new PlayerViewPayload(
                                                    player.getId(),
                                                    "nombre_jugador",
                                                    state.currentPosition().x(),
                                                    state.currentPosition().y(),
                                                    state.direction()));

                                            // Construir el mensaje SUBSCRIBED (respuesta inmediata al cliente)
                                            //Creo que luego podemos darle oro uso a este mensaje de momento lo pusimos para debuguear
                                            //wsMessenger.sendTo(sessionId, MsgType.SUBSCRIBED, new SubscribedPayload(zones));

                                            // Enviar snapshot de las zonas asignadas si hay otras entidades presentes
                                            zones.forEach(zone -> {
                                                Set<UUID> entitiesInZone = presence.getEntitiesInZone(zone);

                                                var players = entitiesInZone.stream()
                                                        .filter(entityId -> !entityId.equals(player.getId()))
                                                        .map(entityStateStore::get)
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
                                            return Mono.empty();
                                        });

                            } catch (JsonProcessingException e) {
                                wsMessenger.sendTo(sessionId, MsgType.ERROR, new ErrorPayload("BAD_JSON", "Invalid Join Payload"));
                                return Mono.error(e);
                            }
                        })
                .onErrorResume(ex -> {
                    // Timeout o parseo inválido: registramos y devolvemos posición por defecto
                    wsMessenger.sendTo(sessionId, MsgType.ERROR, new ErrorPayload("BAD_JSON", "Invalid Join Payload or Timeout"));
                    log.warn("Join_handling_failed -> sessionId={}, remoteAddress={}, error={}", sessionId, remoteAddress, ex.getMessage());
                    // Desconectamos al cliente
                    return session.close().then();
                })
                .then();
    }

}
