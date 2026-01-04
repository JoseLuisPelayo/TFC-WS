package org.jpsoft.tfcws.domain.session;

import org.jpsoft.tfcws.domain.actor.Direction;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;

import java.util.Set;

public record PlayerSessionState(
        String playerId,
        Set<ChunkCoord> currentAOIChunks,
        Position currentPosition,
        ChunkCoord currentChunk,
        Direction direction,
        long createdAtMs,
        long lastUpdatedMs
) {

    // Cambia solo la posición y el updatedAt
    public PlayerSessionState withPosition(Position newPos, Direction direction, Long nowMs) {
        return new PlayerSessionState(
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
    public PlayerSessionState withChunkAndAoi(
            ChunkCoord newChunk,
            Set<ChunkCoord> newAoi,
            Position newPos,
            Direction direction,
            long nowMs
    ) {
        return new PlayerSessionState(
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
