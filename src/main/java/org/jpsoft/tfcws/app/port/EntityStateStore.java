package org.jpsoft.tfcws.app.port;

import org.jpsoft.tfcws.domain.actor.EntityState;

import java.util.HashMap;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface EntityStateStore {
    Optional<EntityState> get(UUID entityId);
    EntityState upsert(UUID entityId, EntityState newState);
    void remove(UUID entityId);
    HashMap<UUID, EntityState> getAllSessions(Set<UUID> ids);
}
