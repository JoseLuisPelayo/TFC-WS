package org.jpsoft.tfcws.domain.session;

import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;

import java.util.Set;

public record PlayerSessionState(
        String playerId,
        Set<ChunkCoord> currentAOIChunks,
        Position currentPosition,
        ChunkCoord currentChunk,
        Long createdAtMs,
        Long lastUpdatedMs
) {
}
