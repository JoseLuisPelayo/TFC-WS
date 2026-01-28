package org.jpsoft.tfcws.app.service;

import lombok.RequiredArgsConstructor;
import org.jpsoft.tfcws.domain.actor.Player;
import org.jpsoft.tfcws.infra.repository.PlayerRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PlayerService {
    private final PlayerRepository players;

    public Mono<Player> getOrCreatePlayer(UUID userId, String name) {
        return players.findByUserIdAndPlayerName(userId, name)
                .switchIfEmpty(Mono.defer(() ->
                        players.save(
                                Player.builder()
                                        .userId(userId)
                                        .playerName(name)
                                        .lastXPosition(0.0F)
                                        .lastYPosition(0.0F)
                                        .createdAt(Instant.now())
                                        .updatedAt(Instant.now())
                                        .build()
                        )
                ));
    }

    public Mono<Set<Player>> getPlayersByUserId(UUID userId) {
        return players.findAllByUserId(userId)
                .collectList()
                .map(Set::copyOf);
    }

    public Mono<Player> savePosition(UUID playerId, double x, double y) {
        Mono<Player> player = players.findById(playerId);
        return player.flatMap(p -> {
            p.setLastXPosition((float) x);
            p.setLastYPosition((float) y);
            p.setUpdatedAt(Instant.now());
            return players.save(p);
        });
    }
}