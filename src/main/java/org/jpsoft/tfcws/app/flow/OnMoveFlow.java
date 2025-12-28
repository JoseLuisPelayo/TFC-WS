package org.jpsoft.tfcws.app.flow;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jpsoft.tfcws.adapter.ws.MsgCodec;
import org.jpsoft.tfcws.app.port.Presence;
import org.jpsoft.tfcws.app.port.SessionRegistry;
import org.jpsoft.tfcws.app.port.SessionStateStore;
import org.jpsoft.tfcws.app.port.dto.AoiSwapResult;
import org.jpsoft.tfcws.domain.session.PlayerSessionState;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.domain.world.ChunkGeometry;
import org.jpsoft.tfcws.ws.msg.*;
import org.jpsoft.tfcws.ws.msg.error.ErrorCode;
import org.jpsoft.tfcws.ws.msg.error.ErrorPayload;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class OnMoveFlow {

    private final MsgCodec codec;
    private final Presence presence;
    private final SessionRegistry sessionRegistry;
    private final SessionStateStore sessionStateStore;

    public Flux<WebSocketMessage> run(WebSocketSession session, Flux<Envelope> input) {

        String playerId = session.getId();
        String sessionId = session.getId();

        return input
                .filter(envelope -> envelope.getType() == MsgType.MOVE)
                .concatMap(envelope -> {
                    PlayerMovePayload payload = null;
                    try {
                        payload = codec.parsePayload(envelope, PlayerMovePayload.class);
                    } catch (JsonProcessingException e) {
                        // Falla el parseo del payload MOVE, respondemos con BAD_MOVE.
                        String json = codec.encode(MsgType.ERROR, new ErrorPayload(ErrorCode.BAD_MOVE.name(), e.getMessage()));
                        return Mono.just(session.textMessage(json));
                    }
                    Position newPosition = new Position(payload.getX(), payload.getY());
                    ChunkCoord activeChunk = ChunkGeometry.posToChunk(newPosition);
                    Optional<PlayerSessionState> state = sessionStateStore.get(sessionId);
                    long now = System.currentTimeMillis();

                    presence.upsertPresence(playerId, newPosition);

                    if (state.isPresent()) {
                        Flux<WebSocketMessage> snapshots = Flux.empty();
                        Mono<WebSocketMessage> despawn = Mono.empty();
                        PlayerSessionState currentState = state.get();

                        if (!currentState.currentChunk().equals(activeChunk)) {
                            AoiSwapResult changedZones = sessionRegistry.swapAoiZones(
                                    sessionId,
                                    activeChunk
                            );

                            sessionStateStore.upsert(sessionId, currentState.withChunkAndAoi(
                                    activeChunk,
                                    ChunkGeometry.getChunksInAOI(activeChunk),
                                    newPosition,
                                    now
                            ));

                            snapshots = Flux.fromIterable(presence.buildSnapShotZone(sessionId, changedZones.enteredZones()))
                                    .map(snapShotZonePayload ->
                                            session.textMessage(codec.encode(MsgType.SNAPSHOT_ZONE, snapShotZonePayload)));

                            despawn = Mono.just(session.textMessage(
                                                    codec.encode(MsgType.DESPAWN_ZONES,
                                                            new DespawnZonesPayload(changedZones.exitedZones())
                                                            )));


                        } else {
                            sessionStateStore.upsert(sessionId, currentState.withPosition(
                                    newPosition,
                                    now
                            ));

                            log.info("Session {} moved to position: {}", sessionId, newPosition);
                        }

                        Mono<WebSocketMessage> move = Mono.just(session.textMessage(
                                codec.encode(MsgType.MOVED,
                                        new PlayerMovedAckPayload(
                                                newPosition.x(),
                                                newPosition.y(),
                                                activeChunk.getZoneKey(),
                                                now
                                        ))));

                        return snapshots.concatWith(despawn).concatWith(move);
                    } else {
                        String json = codec.encode(MsgType.ERROR, new ErrorPayload(ErrorCode.BAD_STATE.name(), ErrorCode.BAD_STATE.defaultMessage()));
                        return Mono.just(session.textMessage(json));
                    }
                });
    }
}
