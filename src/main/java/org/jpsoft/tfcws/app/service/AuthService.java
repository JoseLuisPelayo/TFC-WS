package org.jpsoft.tfcws.app.service;

import lombok.RequiredArgsConstructor;
import org.jpsoft.tfcws.domain.actor.User;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class AuthService {

    private final UserService users;

    public Mono<User> loginOrRegister(String email, String password) {
        return users.getOrCreateByEmailAndPassword(email.trim().toLowerCase(), password);
    }
}
