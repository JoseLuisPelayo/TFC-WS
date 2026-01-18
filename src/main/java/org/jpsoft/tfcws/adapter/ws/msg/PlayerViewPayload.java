package org.jpsoft.tfcws.adapter.ws.msg;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.jpsoft.tfcws.domain.actor.Direction;

import java.util.UUID;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class PlayerViewPayload {
    private UUID playerId;
    private String playerName;
    private double x;
    private double y;
    private Direction direction;
}
