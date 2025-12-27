package org.jpsoft.tfcws.app.port;

import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.ws.msg.SnapShotZonePayload;

import java.util.List;
import java.util.Set;

public interface Presence {
    void upsertPresence(String sessionId, Position position, Set<ChunkCoord> zones);
    void removePresence(String sessionId, Set<ChunkCoord> zones);
    List<SnapShotZonePayload> buildSnapShotZone(String sessionId, Set<ChunkCoord> zones);
}
