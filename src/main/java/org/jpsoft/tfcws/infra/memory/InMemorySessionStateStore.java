package org.jpsoft.tfcws.infra.memory;

import org.jpsoft.tfcws.app.port.SessionStateStore;
import org.jpsoft.tfcws.domain.actor.EntityState;

import java.util.HashMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemorySessionStateStore implements SessionStateStore {

    private final ConcurrentMap<String, EntityState> storeById = new ConcurrentHashMap<>();

    @Override
    public Optional<EntityState> get(String id) {
        return Optional.ofNullable(storeById.get(id));
    }

    @Override
    public EntityState upsert(String id, EntityState state) {
        storeById.put(id, state);
        return state;
    }

    @Override
    public void remove(String id) {
        storeById.remove(id);
    }

    public HashMap<String, EntityState> getAllSessions(Set<String> ids) {
        return ids.stream()
                .filter(storeById::containsKey)
                .collect(HashMap::new,
                        (map, id) -> map.put(id, storeById.get(id)),
                        HashMap::putAll);
    }
}
