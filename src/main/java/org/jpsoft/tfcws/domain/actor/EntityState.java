package org.jpsoft.tfcws.domain.actor;

import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;

import java.util.Set;

public record EntityState(
        String playerId,
        Set<ChunkCoord> currentAOIChunks,
        Position currentPosition,
        ChunkCoord currentChunk,
        Direction direction,
        long createdAtMs,
        long lastUpdatedMs
) {

    // Cambia solo la posición y el updatedAt
    public EntityState withPosition(Position newPos, Direction direction, Long nowMs) {
        return new EntityState(
                playerId,
                currentAOIChunks,
                newPos,
                currentChunk,
                direction,
                createdAtMs,
                nowMs
        );
    }

    // Cambia chunk + AOI + position, y el updatedAt
    public EntityState withChunkAndAoi(
            ChunkCoord newChunk,
            Set<ChunkCoord> newAoi,
            Position newPos,
            Direction direction,
            long nowMs
    ) {
        return new EntityState(
                playerId,
                newAoi,
                newPos,
                newChunk,
                direction,
                createdAtMs,
                nowMs
        );
    }
}
