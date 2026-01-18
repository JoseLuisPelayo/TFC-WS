package org.jpsoft.tfcws.infra.repository;

import org.jpsoft.tfcws.domain.actor.Player;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Repository
public interface PlayerRepository extends ReactiveCrudRepository<Player, String> {
    Mono<Player> findByUserIdAndPlayerName(UUID userId, String playerName);

    Flux<Player> findAllByUserId(UUID userId);

    Mono<Player> findById(UUID playerId);
}
