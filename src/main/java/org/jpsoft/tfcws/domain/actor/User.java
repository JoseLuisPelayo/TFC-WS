package org.jpsoft.tfcws.domain.actor;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "users", schema = "warfarm")
public class User {

    @Id
    private String id;

    private String email;

    private String passwordHash;

    private Instant createdAt;

    private Instant updatedAt;

}
