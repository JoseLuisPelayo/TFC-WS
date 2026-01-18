package org.jpsoft.tfcws.adapter.ws.msg;

import org.jpsoft.tfcws.domain.actor.Player;

import java.util.Set;
import java.util.UUID;

public record AuthOkPayload(
        UUID userId,
        Set<Player> players
) {
}
