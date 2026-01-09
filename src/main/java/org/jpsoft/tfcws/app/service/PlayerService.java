package org.jpsoft.tfcws.app.service;

import lombok.RequiredArgsConstructor;
import org.jpsoft.tfcws.domain.actor.Player;
import org.jpsoft.tfcws.infra.repository.PlayerRepository;
import reactor.core.publisher.Mono;

import java.time.Instant;

@RequiredArgsConstructor
public class PlayerService {
    private final PlayerRepository players;

    public Mono<Player> getOrCreatePlayer(String userId, String name) {
        return players.findByUserId(userId)
                .switchIfEmpty(Mono.defer(() -> {
                    return players.save(
                            Player.builder()
                                    .id(userId)
                                    .playerName(name)
                                    .lastXPosition(0.0F)
                                    .lastYPosition(0.0F)
                                    .createdAt(java.time.Instant.now())
                                    .updatedAt(java.time.Instant.now())
                                    .build()
                    );
                }));
    }

    public Mono<Player> savePosition(String playerId, double x, double y) {
        return players.findById(playerId)
                .flatMap(p -> {
                    p.setLastXPosition(x);
                    p.setLastYPosition(y);
                    p.setUpdatedAt(Instant.now());
                    return players.save(p);
                });
    }
}
