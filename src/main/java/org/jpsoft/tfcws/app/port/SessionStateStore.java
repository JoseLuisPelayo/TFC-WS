package org.jpsoft.tfcws.app.port;

import org.jpsoft.tfcws.domain.actor.SessionState;

import java.util.Optional;

public interface SessionStateStore {

    void bind(String sessionId, SessionState state);

    Optional<SessionState> getSessionState(String sessionId);

    void unbind(String sessionId);

}
