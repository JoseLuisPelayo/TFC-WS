package org.jpsoft.tfcws.app.service;

import org.jpsoft.tfcws.domain.actor.User;
import org.jpsoft.tfcws.infra.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UserServiceTest {

    @Mock
    private UserRepository users;

    private UserService service;

    @BeforeEach
    void setUp() {
        service = new UserService(users);
    }

    @Test
    void getOrCreate_existingUser_returnsIt_andDoesNotSave() {
        var existing = User.builder()
                .email("a@b.com")
                .passwordHash("hash")
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        when(users.findByEmail("a@b.com")).thenReturn(Mono.just(existing));

        StepVerifier.create(service.getOrCreateByEmailAndPassword("a@b.com", "newHash"))
                .expectNext(existing)
                .verifyComplete();

        verify(users).findByEmail("a@b.com");
        verify(users, never()).save(any());
    }

    @Test
    void getOrCreate_missingUser_savesNew_withTrimmedEmail_andTimestamps() {
        String rawEmail = "  a@b.com  ";
        String trimmed = "a@b.com";
        String hash = "hash123";

        when(users.findByEmail(trimmed)).thenReturn(Mono.empty());

        // Simulamos que el repo devuelve lo mismo que le pasan pero con "id" si existiera.
        when(users.save(any(User.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.getOrCreateByEmailAndPassword(rawEmail, hash))
                .assertNext(saved -> {
                    // email trimmed
                    org.junit.jupiter.api.Assertions.assertEquals(trimmed, saved.getEmail());
                    org.junit.jupiter.api.Assertions.assertEquals(hash, saved.getPasswordHash());

                    // timestamps creados
                    org.junit.jupiter.api.Assertions.assertNotNull(saved.getCreatedAt());
                    org.junit.jupiter.api.Assertions.assertNotNull(saved.getUpdatedAt());
                })
                .verifyComplete();

        ArgumentCaptor<User> captor = ArgumentCaptor.forClass(User.class);
        verify(users).save(captor.capture());
        User toSave = captor.getValue();

        org.junit.jupiter.api.Assertions.assertEquals(trimmed, toSave.getEmail());
        org.junit.jupiter.api.Assertions.assertEquals(hash, toSave.getPasswordHash());
        org.junit.jupiter.api.Assertions.assertNotNull(toSave.getCreatedAt());
        org.junit.jupiter.api.Assertions.assertNotNull(toSave.getUpdatedAt());
    }
}
