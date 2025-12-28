package org.jpsoft.tfcws.infra.memory;

import org.jpsoft.tfcws.app.port.SessionStateStore;
import org.jpsoft.tfcws.domain.session.PlayerSessionState;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemorySessionStateStore implements SessionStateStore {

    private final ConcurrentMap<String, PlayerSessionState> storeBySession = new ConcurrentHashMap<>();

    @Override
    public Optional<PlayerSessionState> get(String sessionId) {
        return Optional.ofNullable(storeBySession.get(sessionId));
    }

    @Override
    public PlayerSessionState upsert(String sessionId, PlayerSessionState state) {
        storeBySession.put(sessionId, state);
        return state;
    }

    @Override
    public void remove(String sessionId) {
        storeBySession.remove(sessionId);
    }
}
