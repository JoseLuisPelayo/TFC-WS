package org.jpsoft.tfcws.domain.actor;

import lombok.*;

import java.util.UUID;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SessionState {
    private String sessionId;
    private UUID userId;
    private UUID playerId;
}
