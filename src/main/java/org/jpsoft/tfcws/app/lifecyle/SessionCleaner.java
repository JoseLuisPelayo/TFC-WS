package org.jpsoft.tfcws.app.lifecyle;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jpsoft.tfcws.app.port.*;
import org.jpsoft.tfcws.app.service.PlayerService;
import org.jpsoft.tfcws.domain.actor.EntityState;
import org.jpsoft.tfcws.domain.actor.SessionState;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.infra.repository.ticker.PositionTicker;
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
    private final PositionTicker positionTicker;

    public Mono<Void> clean(String sessionId) {

        Optional<SessionState> optSessionState = sessionStateStore.getSessionState(sessionId);
        if (optSessionState.isEmpty()) {
            log.info("SessionCleaner: no session state for sessionId={}", sessionId);

            // Aun así, limpia lo básico por sesión
            return Mono.fromRunnable(() -> {
                sessionRegistry.removeSession(sessionId);
                outboundHub.unregister(sessionId);
                sessionStateStore.unbind(sessionId);
            });
        }

        SessionState ss = optSessionState.get();
        UUID playerId = ss.getPlayerId();

        Mono<Void> persistIfPossible = Mono.defer(() -> {
            if (playerId == null) return Mono.empty();

            Optional<EntityState> optEntityState = entityStateStore.get(playerId);
            if (optEntityState.isEmpty()) {
                log.debug("SessionCleaner: no entity state for playerId={}", playerId);
                positionTicker.removePlayer(playerId);
                return Mono.empty();
            }

            EntityState es = optEntityState.get();
            Position pos = es.currentPosition();
            if (pos == null) {
                positionTicker.removePlayer(playerId);
                return Mono.empty();
            }

            log.info("SessionCleaner: persisting last position playerId={} x={} y={}",
                    playerId, pos.x(), pos.y());

            positionTicker.removePlayer(playerId);

            return playerService.savePosition(playerId, pos.x(), pos.y())
                    .onErrorResume(err -> {
                        // No bloquees cleanup por fallo DB
                        log.warn("SessionCleaner: failed to persist position for playerId={}", playerId, err);
                        return Mono.empty();
                    })
                    .then();
        });

        Mono<Void> cleanupAlways = Mono.fromRunnable(() -> {
            log.info("SessionCleaner: cleaning sessionId={} playerId={}", sessionId, playerId);

            if (playerId != null) {
                presence.removePresence(playerId);
                entityStateStore.remove(playerId);
                positionTicker.removePlayer(playerId);
            }

            sessionRegistry.removeSession(sessionId);
            outboundHub.unregister(sessionId);
            sessionStateStore.unbind(sessionId);
        });

        // Persistir y luego limpiar siempre
        return persistIfPossible.then(cleanupAlways);
    }
}
