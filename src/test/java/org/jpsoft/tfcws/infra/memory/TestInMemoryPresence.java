package org.jpsoft.tfcws.infra.memory;

import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.domain.world.ChunkGeometry;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TestInMemoryPresence {

    private final ConcurrentHashMap<String, Position> positionBySession = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Map<String, Position>> presenceByZone = new ConcurrentHashMap<>();

    @Test
    void upsertPresence() {
        //ARRANGE
        String sessionId = "session1";
        Position position = new Position(10, 20);
        Set<ChunkCoord> zones = ChunkGeometry.getChunksInAOI(ChunkGeometry.posToChunk(position));

        //ACT
        positionBySession.put(sessionId, position);
        for (ChunkCoord zone : zones) {
            presenceByZone.computeIfAbsent(zone.getZoneKey(), k -> new HashMap<>())
                    .put(sessionId, position);
        }

        //ASSERT
        assert positionBySession.get(sessionId).equals(position);
        for (ChunkCoord zone : zones) {
            assert presenceByZone.get(zone.getZoneKey()).get(sessionId).equals(position);
        }

        assert presenceByZone.size() == 9;
    }

}
