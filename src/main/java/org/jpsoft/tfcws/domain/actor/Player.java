package org.jpsoft.tfcws.domain.actor;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Table(name = "players", schema = "warfarm")
public class Player {
    @Id
    private UUID id;

    private UUID userId;

    private String playerName;

    private double lastXPosition;

    private double lastYPosition;

    private Instant createdAt;

    private Instant updatedAt;

}
