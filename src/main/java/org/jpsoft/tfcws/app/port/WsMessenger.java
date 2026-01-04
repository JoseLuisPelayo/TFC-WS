package org.jpsoft.tfcws.app.port;

import org.jpsoft.tfcws.adapter.ws.msg.MsgType;
import org.jpsoft.tfcws.domain.world.ChunkCoord;

public interface WsMessenger {

    void sendTo(String sessionId, MsgType type, Object payload);

    void broadcastToZone(ChunkCoord zone, MsgType type, Object payload);

}
