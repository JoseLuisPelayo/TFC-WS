package org.jpsoft.tfcws.infra.repository.ticker;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jpsoft.tfcws.app.port.EntityStateStore;
import org.jpsoft.tfcws.app.port.SessionStateStore;
import org.jpsoft.tfcws.app.service.PlayerService;
import org.jpsoft.tfcws.domain.actor.EntityState;
import org.jpsoft.tfcws.domain.actor.SessionState;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@RequiredArgsConstructor
@Component
public class PositionTicker implements SmartLifecycle {

    private final PlayerService playerService;
    private final SessionStateStore sessionStateStore;
    private final EntityStateStore entityStateStore;

    private final Duration tickDuration = Duration.ofSeconds(5);
    private Disposable subscription;
    private volatile boolean running = false;
    private final ConcurrentHashMap<UUID, Position> lastKnownPositionsByPlayerId = new ConcurrentHashMap<>();

    @Override
    public void start() {
        if (running) return;
        running = true;

        subscription = Flux.interval(tickDuration)
                .flatMap(tick -> {
                    Set<SessionState> sessionStates = sessionStateStore.getAllSessionStates();
                    if (sessionStates.isEmpty()) {
                        log.debug("No active session states found during position ticker tick.");
                        return Flux.empty();
                    }
                     return Flux.fromIterable(sessionStateStore.getAllSessionStates());
                })
                .filter(sessionState -> sessionState.getPlayerId() != null)
                .map(SessionState::getPlayerId)
                .flatMap(playerId -> {
                    Optional<EntityState> optionalEntityState = entityStateStore.get(playerId);
                    if (optionalEntityState.isEmpty()) {
                        log.error("No entity state found for userId: {} the state store was not synchronized", playerId);
                        return Mono.empty();
                    }
                    EntityState entityState = optionalEntityState.get();

                    if (lastKnownPositionsByPlayerId.get(playerId) == null || !lastKnownPositionsByPlayerId.get(playerId).equals(entityState.currentPosition())) {
                        lastKnownPositionsByPlayerId.put(playerId, entityState.currentPosition());
                        log.debug("Position ticker updated position for playerId: {}", playerId);
                        return playerService.savePosition(
                                entityState.playerId(),
                                entityState.currentPosition().x(),
                                entityState.currentPosition().y());

                    }

                    return Mono.empty();
                })
                .onErrorContinue((err, obj) -> {
                    log.error("Error occurred during position ticker operation on object: {}", obj, err);
                })
                .subscribe();
    }

    @Override
    public void stop() {
        running = false;
        if (subscription != null && !subscription.isDisposed())
            subscription.dispose();

    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public void removePlayer(UUID playerId) {
        lastKnownPositionsByPlayerId.remove(playerId);
    }
}
