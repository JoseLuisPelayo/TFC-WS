package org.jpsoft.tfcws.app.flow;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jpsoft.tfcws.adapter.ws.MsgCodec;
import org.jpsoft.tfcws.adapter.ws.msg.*;
import org.jpsoft.tfcws.app.port.Presence;
import org.jpsoft.tfcws.app.port.SessionRegistry;
import org.jpsoft.tfcws.app.port.SessionStateStore;
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
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class OnMoveFlow {

    private final MsgCodec codec;
    private final Presence presence;
    private final SessionStateStore sessionStateStore;
    private final SessionRegistry sessionRegistry;
    private final WsMessenger wsMessenger;


    public Mono<Void> run(WebSocketSession session, Flux<Envelope> bus) {

        String playerId = session.getId();
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
                        return Mono.empty();
                    }
                    Position newPosition = new Position(payload.getX(), payload.getY());
                    ChunkCoord activeChunk = ChunkGeometry.posToChunk(newPosition);
                    long now = System.currentTimeMillis();
                    Optional<EntityState> opt = sessionStateStore.get(sessionId);

                    if (opt.isEmpty()) {
                        wsMessenger.sendTo(sessionId, MsgType.ERROR,
                                new ErrorPayload(ErrorCode.BAD_STATE.name(), ErrorCode.BAD_STATE.defaultMessage()));
                        return Mono.empty();
                    }
                    EntityState currentState = opt.get();

                    presence.upsertPresence(playerId, newPosition);

                    if (!currentState.currentChunk().equals(activeChunk)) {
                        AoiSwapResult changedZones = sessionRegistry.swapAoiZones(
                                sessionId,
                                activeChunk
                        );

                        sessionStateStore.upsert(sessionId, currentState.withChunkAndAoi(
                                activeChunk,
                                ChunkGeometry.getChunksInAOI(activeChunk),
                                newPosition,
                                payload.getDirection(),
                                now
                        ));

                        // Enviar snapshot de las nuevas zonas ingresadas
                        changedZones.enteredZones().forEach(zone -> {
                            Set<String> entitiesInZone = presence.getEntitiesInZone(zone);
                            HashMap<String, EntityState> states = sessionStateStore.getAllSessions(entitiesInZone);

                            wsMessenger.sendTo(sessionId, MsgType.SNAPSHOT_ZONE, new SnapShotZonePayload(zone.getZoneKey(), entitiesInZone.stream()
                                    .filter(entityId -> !entityId.equals(sessionId))
                                    .map(id -> {
                                        EntityState state = states.get(id);
                                        return new PlayerViewPayload(
                                                state.playerId(),
                                                "nombre_jugador",
                                                state.currentPosition().x(),
                                                state.currentPosition().y(),
                                                state.direction());
                                    }).toList()));
                        });

                        changedZones.exitedZones().forEach(zone -> {
                            Set<String> entitiesInZone = presence.getEntitiesInZone(zone);
                            wsMessenger.sendTo(sessionId, MsgType.DESPAWN_ENTITIES, new DespawnPlayerPayload(entitiesInZone));
                        });

                        // 2) Despawn a los demas jugadores que salieron de las zonas
                        Set<String> watchers = changedZones.exitedZones().stream()
                                .flatMap(zone -> sessionRegistry.getSessionsByZone(zone).stream())
                                .filter(w -> !w.equals(sessionId))
                                .collect(Collectors.toSet());

                        watchers.forEach(watcherSession ->
                                wsMessenger.sendTo(watcherSession, MsgType.DESPAWN_ENTITIES, new DespawnPlayerPayload(Set.of(playerId)))
                        );

                    } else {
                        sessionStateStore.upsert(sessionId, currentState.withPosition(
                                newPosition,
                                payload.getDirection(),
                                now
                        ));

                        log.info("Session {} moved to position: {}", sessionId, newPosition);
                    }

                    wsMessenger.broadcastToZone(
                            activeChunk,
                            MsgType.PLAYER_MOVED,
                            new PlayerMovedPayload(
                                    playerId,
                                    newPosition.x(),
                                    newPosition.y(),
                                    now
                            ));


                    return Mono.empty();
                })
                .then();
    }

}
