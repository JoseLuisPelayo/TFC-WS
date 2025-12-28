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

    // Cambia solo la posición y el updatedAt
    public PlayerSessionState withPosition(Position newPos, Long nowMs) {
        return new PlayerSessionState(
                playerId,
                currentAOIChunks,
                newPos,
                currentChunk,
                createdAtMs,
                nowMs
        );
    }

    // Cambia chunk + AOI + position, y el updatedAt
    public PlayerSessionState withChunkAndAoi(
            ChunkCoord newChunk,
            Set<ChunkCoord> newAoi,
            Position newPos,
            long nowMs
    ) {
        return new PlayerSessionState(
                playerId,
                newAoi, // defensa si te pasan algo mutable
                newPos,
                newChunk,
                createdAtMs,
                nowMs
        );
    }
}
