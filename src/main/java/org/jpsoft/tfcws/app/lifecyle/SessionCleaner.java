package org.jpsoft.tfcws.app.lifecyle;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jpsoft.tfcws.app.port.*;
import org.jpsoft.tfcws.app.service.PlayerService;
import org.jpsoft.tfcws.domain.actor.EntityState;
import org.jpsoft.tfcws.domain.actor.SessionState;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class SessionCleaner {

    private final SessionStateStore sessionStateStore;
    private final OutboundHub outboundHub;
    private final Presence presence;
    private final EntityStateStore entityStateStore;
    private final SessionRegistry sessionRegistry;
    private final PlayerService playerService;

    public Mono<Void> clean(String sessionId) {
        Optional<SessionState> optSessionState = sessionStateStore.getSessionState(sessionId);
        if (optSessionState.isEmpty()) {
            log.info("No session state found for sessionId: {}", sessionId);
            return Mono.empty();
        }
        SessionState sessionState = optSessionState.get();

        Optional<EntityState> optEntityState = entityStateStore.get(sessionState.getPlayerId());
        if (optEntityState.isEmpty()) {
            log.info("No entity state found for playerId: {}", sessionState.getPlayerId());
            return Mono.empty();
        }

        EntityState entityState = optEntityState.get();
        UUID playerId = entityState.playerId();
        Position lastPosition = entityState.currentPosition();

        Mono<Void> persistPosition = Mono.defer(() -> {
            log.info("Persisting last position for playerId {}: x={}, y={}",
                    playerId, lastPosition.x(), lastPosition.y());
            return playerService.savePosition(playerId, lastPosition.x(), lastPosition.y()).then();
     });

        log.info("Cleaning session for sessionId: {}, playerId: {}", sessionId, playerId);

        Mono<Void> clean = Mono.fromRunnable(() -> {
            presence.removePresence(playerId);
            sessionRegistry.removeSession(sessionId);
            outboundHub.unregister(sessionId);
            entityStateStore.remove(playerId);
            sessionStateStore.unbind(sessionId);
        });

        return clean.then(persistPosition).then();
    }
}
