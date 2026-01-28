package org.jpsoft.tfcws.infra.memory;

import org.jpsoft.tfcws.app.port.EntityStateStore;
import org.jpsoft.tfcws.domain.actor.EntityState;

import java.util.HashMap;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryEntityStateStore implements EntityStateStore {

    private final ConcurrentMap<UUID, EntityState> storeById = new ConcurrentHashMap<>();

    @Override
    public Optional<EntityState> get(UUID id) {
        return Optional.ofNullable(storeById.get(id));
    }

    @Override
    public EntityState upsert(UUID id, EntityState state) {
        storeById.put(id, state);
        return state;
    }

    @Override
    public void remove(UUID id) {
        storeById.remove(id);
    }

    public HashMap<UUID, EntityState> getAllSessions(Set<UUID> ids) {
        HashMap<UUID, EntityState> result = new HashMap<>();
        for (UUID id : ids) {
            EntityState state = storeById.get(id);
            if (state != null) result.put(id, state);
        }
        return result;
    }
}
