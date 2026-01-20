package org.jpsoft.tfcws.app.port;

import org.jpsoft.tfcws.domain.actor.SessionState;

import java.util.Optional;
import java.util.Set;

public interface SessionStateStore {

    void bind(String sessionId, SessionState state);

    Optional<SessionState> getSessionState(String sessionId);

    Set<SessionState> getAllSessionStates();

    void unbind(String sessionId);

}
