package org.jpsoft.tfcws.app.service;

import lombok.RequiredArgsConstructor;
import org.jpsoft.tfcws.domain.actor.User;
import org.jpsoft.tfcws.infra.repository.UserRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository users;

    public Mono<User> getOrCreateByEmailAndPassword(String email, String passwordHash) {
        return users.findByEmail(email.trim())
                .switchIfEmpty(Mono.defer(() -> {
                    var now = Instant.now();
                    return users.save(
                            User.builder()
                                    .email(email.trim())
                                    .passwordHash(passwordHash)
                                    .createdAt(now)
                                    .updatedAt(now)
                                    .build()
                    );
                }));
    }
}