package org.jpsoft.tfcws.infra.memory;

import org.jpsoft.tfcws.app.port.SessionStateStore;
import org.jpsoft.tfcws.domain.actor.SessionState;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InMemorySessionStateStore implements SessionStateStore {

    private final ConcurrentHashMap<String, SessionState> storeBySessionId = new ConcurrentHashMap<>();

    @Override
    public void bind(String sessionId, SessionState state) {
        if (sessionId == null || sessionId.isBlank()) throw new IllegalArgumentException("sessionId");
        if (state == null) throw new IllegalArgumentException("state");
        storeBySessionId.put(sessionId, state);
    }

    @Override
    public Optional<SessionState> getSessionState(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) return Optional.empty();
        return Optional.ofNullable(storeBySessionId.get(sessionId));
    }

    @Override
    public Set<SessionState> getAllSessionStates() {
        return Set.copyOf(storeBySessionId.values());
    }

    @Override
    public void unbind(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) throw new IllegalArgumentException("sessionId");
        storeBySessionId.remove(sessionId);
    }
}
