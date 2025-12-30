package org.jpsoft.tfcws.app.port;

import org.jpsoft.tfcws.domain.session.PlayerSessionState;

import java.util.Optional;

public interface SessionStateStore {
    Optional<PlayerSessionState> get(String sessionId);
    PlayerSessionState upsert(String sessionId, PlayerSessionState newState);
    void remove(String sessionId);
}
