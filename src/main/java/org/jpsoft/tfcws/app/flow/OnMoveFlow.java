package org.jpsoft.tfcws.app.flow;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jpsoft.tfcws.adapter.ws.MsgCodec;
import org.jpsoft.tfcws.adapter.ws.msg.*;
import org.jpsoft.tfcws.app.port.Presence;
import org.jpsoft.tfcws.app.port.SessionRegistry;
import org.jpsoft.tfcws.app.port.EntityStateStore;
import org.jpsoft.tfcws.app.port.WsMessenger;
import org.jpsoft.tfcws.app.port.dto.AoiSwapResult;
import org.jpsoft.tfcws.domain.actor.EntityState;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.domain.world.ChunkGeometry;
import org.jpsoft.tfcws.adapter.ws.msg.error.ErrorCode;
import org.jpsoft.tfcws.adapter.ws.msg.error.ErrorPayload;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class OnMoveFlow {

    private final MsgCodec codec;
    private final Presence presence;
    private final EntityStateStore entityStateStore;
    private final SessionRegistry sessionRegistry;
    private final WsMessenger wsMessenger;

    public Mono<Void> run(WebSocketSession session, Flux<Envelope> bus) {

        String sessionId = session.getId();

        return bus
                .filter(envelope -> envelope.getType() == MsgType.PLAYER_MOVE)
                .concatMap(envelope -> {
                    PlayerMovePayload payload = null;
                    try {
                        payload = codec.parsePayload(envelope, PlayerMovePayload.class);
                    } catch (JsonProcessingException e) {
                        wsMessenger.sendTo(sessionId, MsgType.ERROR,
                                new ErrorPayload(ErrorCode.BAD_MOVE.name(), e.getMessage()));
                        log.error("Failed to parse PlayerMovePayload for sessionId: {}", sessionId, e);
                        return Mono.empty();
                    }

                    log.info("Received move payload from sessionId {}: x={}, y={}, direction={}",
                            sessionId, payload.getX(), payload.getY(), payload.getDirection());

                    // Actualizar la posicion del jugador
                    Position newPosition = new Position(payload.getX(), payload.getY());
                    ChunkCoord activeChunk = ChunkGeometry.posToChunk(newPosition);
                    long now = System.currentTimeMillis();
                    Optional<EntityState> opt = entityStateStore.get(payload.getPlayerId());

                    if (opt.isEmpty()) {
                        wsMessenger.sendTo(sessionId, MsgType.ERROR,
                                new ErrorPayload(ErrorCode.BAD_STATE.name(), ErrorCode.BAD_STATE.defaultMessage()));

                        log.error("No session state found for sessionId: {}", sessionId);
                        return Mono.empty();
                    }
                    EntityState currentState = opt.get();

                    presence.upsertPresence(currentState.playerId(), newPosition);

                    wsMessenger.broadcastToZone(
                            activeChunk,
                            MsgType.PLAYER_MOVED,
                            new PlayerMovedPayload(
                                    currentState.playerId(),
                                    newPosition.x(),
                                    newPosition.y(),
                                    payload.getDirection(),
                                    now
                            ));

                    entityStateStore.upsert(currentState.playerId(), currentState.withPosition(
                            newPosition,
                            payload.getDirection(),
                            now
                    ));

                    if (!currentState.currentChunk().equals(activeChunk)) {
                        AoiSwapResult changedZones = sessionRegistry.swapAoiZones(
                                sessionId,
                                activeChunk
                        );

                        entityStateStore.upsert(currentState.playerId(), currentState.withChunkAndAoi(
                                activeChunk,
                                ChunkGeometry.getChunksInAOI(activeChunk),
                                newPosition,
                                payload.getDirection(),
                                now
                        ));

                        changedZones.enteredZones().forEach(zone -> {
                            Set<UUID> entitiesInZone = presence.getEntitiesInZone(zone);
                            HashMap<UUID, EntityState> states = entityStateStore.getAllSessions(entitiesInZone);

                            var players = entitiesInZone.stream()
                                    .filter(id -> !id.equals(currentState.playerId()))
                                    .map(states::get)
                                    .filter(java.util.Objects::nonNull)
                                    .map(state -> new PlayerViewPayload(
                                            state.playerId(),
                                            "nombre_jugador",
                                            state.currentPosition().x(),
                                            state.currentPosition().y(),
                                            state.direction()
                                    ))
                                    .toList();

                            if (!players.isEmpty()) {
                                wsMessenger.sendTo(sessionId, MsgType.SNAPSHOT_ZONE,
                                        new SnapShotZonePayload(zone.getZoneKey(), players));
                            }
                        });

                        // 1) Despawn al jugador que salio de las zonas
                        changedZones.exitedZones().forEach(zone -> {
                            Set<UUID> entitiesInZone = presence.getEntitiesInZone(zone);
                            if (!entitiesInZone.isEmpty())
                                wsMessenger.sendTo(
                                        sessionId,
                                        MsgType.DESPAWN_ENTITIES,
                                        new DespawnPlayerPayload(entitiesInZone)
                                );
                        });

                        // 2) Despawn a los demas jugadores que salieron de las zonas
                        Set<String> watchers = changedZones.exitedZones().stream()
                                .flatMap(zone -> sessionRegistry.getSessionsByZone(zone).stream())
                                .filter(w -> !w.equals(sessionId))
                                .collect(Collectors.toSet());

                        if (!watchers.isEmpty()) {
                            watchers.forEach(watcherSession ->
                                    wsMessenger.sendTo(watcherSession, MsgType.DESPAWN_ENTITIES, new DespawnPlayerPayload(Set.of(currentState.playerId())))
                            );
                        }
                    }

                    return Mono.empty();
                })

                .then();
    }
}
