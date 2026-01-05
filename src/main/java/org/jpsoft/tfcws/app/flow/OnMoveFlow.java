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
import org.jpsoft.tfcws.domain.session.PlayerSessionState;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.domain.world.ChunkGeometry;
import org.jpsoft.tfcws.adapter.ws.msg.error.ErrorCode;
import org.jpsoft.tfcws.adapter.ws.msg.error.ErrorPayload;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;
import java.util.Set;

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
                    Optional<PlayerSessionState> opt = sessionStateStore.get(sessionId);

                    if (opt.isEmpty()) {
                        wsMessenger.sendTo(sessionId, MsgType.ERROR,
                                new ErrorPayload(ErrorCode.BAD_STATE.name(), ErrorCode.BAD_STATE.defaultMessage()));
                        return Mono.empty();
                    }
                    PlayerSessionState currentState = opt.get();

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
                                    List<PlayerViewPayload> aux = entitiesInZone.stream().map(
                                            entityId -> {
                                                if (!entityId.equals(sessionId)) {
                                                    Optional<PlayerSessionState> psState = sessionStateStore.get(entityId);
                                                    if (psState.isPresent()) {
                                                        PlayerSessionState otherState = psState.get();
                                                        return new PlayerViewPayload(
                                                                otherState.playerId(),
                                                                "nombre_jugador",
                                                                otherState.currentPosition().x(),
                                                                otherState.currentPosition().y(),
                                                                otherState.direction()
                                                        );
                                                    }
                                                }
                                                return new PlayerViewPayload();
                                            }
                                    ).toList();

                                    wsMessenger.sendTo(sessionId, MsgType.SNAPSHOT_ZONE, new SnapShotZonePayload(zone.getZoneKey(), aux));
                                }
                        );

                        wsMessenger.sendTo(
                                sessionId,
                                MsgType.DESPAWN_ZONES,
                                new DespawnZonesPayload(changedZones.exitedZones()));
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
