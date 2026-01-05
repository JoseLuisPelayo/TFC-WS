package org.jpsoft.tfcws.app.port;

import org.jpsoft.tfcws.domain.actor.EntityState;

import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

public interface SessionStateStore {
    Optional<EntityState> get(String sessionId);
    EntityState upsert(String sessionId, EntityState newState);
    void remove(String sessionId);
    HashMap<String, EntityState> getAllSessions(Set<String> ids);
}
