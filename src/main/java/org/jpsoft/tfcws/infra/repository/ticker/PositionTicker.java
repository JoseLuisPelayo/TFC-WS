package org.jpsoft.tfcws.infra.repository.ticker;

import lombok.RequiredArgsConstructor;
import org.jpsoft.tfcws.app.port.SessionStateStore;
import org.jpsoft.tfcws.app.service.PlayerService;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class PositionTicker implements SmartLifecycle {

    private final PlayerService playerService;
    private final SessionStateStore sessionStateStore;
    private final

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isRunning() {
        return false;
    }
}
