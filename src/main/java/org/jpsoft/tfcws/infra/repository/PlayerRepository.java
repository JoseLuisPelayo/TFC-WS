package org.jpsoft.tfcws.infra.repository;

import org.jpsoft.tfcws.domain.actor.Player;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface PlayerRepository extends ReactiveCrudRepository<Player, String> {
    Mono<Player> findByUserId(String userId);
}
