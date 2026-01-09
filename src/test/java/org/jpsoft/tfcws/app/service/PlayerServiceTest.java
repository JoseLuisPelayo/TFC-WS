package org.jpsoft.tfcws.app.service;

import org.jpsoft.tfcws.domain.actor.Player;
import org.jpsoft.tfcws.infra.repository.PlayerRepository;
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
class PlayerServiceTest {

    @Mock
    private PlayerRepository players;

    private PlayerService service;

    @BeforeEach
    void setUp() {
        service = new PlayerService(players);
    }

    @Test
    void getOrCreatePlayer_existing_returnsIt_andDoesNotSave() {
        var existing = Player.builder()
                .id("playerId")
                .playerName("Nana")
                .lastXPosition(10.0F)
                .lastYPosition(20.0F)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        when(players.findByUserId("user-1")).thenReturn(Mono.just(existing));

        StepVerifier.create(service.getOrCreatePlayer("user-1", "NewName"))
                .expectNext(existing)
                .verifyComplete();

        verify(players).findByUserId("user-1");
        verify(players, never()).save(any());
    }

    @Test
    void getOrCreatePlayer_missing_savesNew_withDefaults() {
        when(players.findByUserId("user-1")).thenReturn(Mono.empty());
        when(players.save(any(Player.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.getOrCreatePlayer("user-1", "Nana"))
                .assertNext(saved -> {
                    // OJO: tu código hace id=userId. Lo comprobamos tal cual.
                    org.junit.jupiter.api.Assertions.assertEquals("user-1", saved.getId());
                    org.junit.jupiter.api.Assertions.assertEquals("Nana", saved.getPlayerName());
                    org.junit.jupiter.api.Assertions.assertEquals(0.0F, saved.getLastXPosition());
                    org.junit.jupiter.api.Assertions.assertEquals(0.0F, saved.getLastYPosition());
                    org.junit.jupiter.api.Assertions.assertNotNull(saved.getCreatedAt());
                    org.junit.jupiter.api.Assertions.assertNotNull(saved.getUpdatedAt());
                })
                .verifyComplete();

        verify(players).save(any(Player.class));
    }

    @Test
    void savePosition_existing_updatesAndSaves() {
        var existing = Player.builder()
                .id("p1")
                .playerName("Nana")
                .lastXPosition(0.0F)
                .lastYPosition(0.0F)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        when(players.findById("p1")).thenReturn(Mono.just(existing));
        when(players.save(any(Player.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        StepVerifier.create(service.savePosition("p1", 12.5, -7.0))
                .assertNext(saved -> {
                    org.junit.jupiter.api.Assertions.assertEquals(12.5F, saved.getLastXPosition());
                    org.junit.jupiter.api.Assertions.assertEquals(-7.0F, saved.getLastYPosition());
                    org.junit.jupiter.api.Assertions.assertNotNull(saved.getUpdatedAt());
                })
                .verifyComplete();

        ArgumentCaptor<Player> captor = ArgumentCaptor.forClass(Player.class);
        verify(players).save(captor.capture());
        Player toSave = captor.getValue();
        org.junit.jupiter.api.Assertions.assertEquals(12.5F, toSave.getLastXPosition());
        org.junit.jupiter.api.Assertions.assertEquals(-7.0F, toSave.getLastYPosition());
    }

    @Test
    void savePosition_missing_returnsEmpty_andDoesNotSave() {
        when(players.findById("p1")).thenReturn(Mono.empty());

        StepVerifier.create(service.savePosition("p1", 1, 2))
                .verifyComplete(); // no items

        verify(players, never()).save(any());
    }
}
