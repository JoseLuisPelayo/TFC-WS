package org.jpsoft.tfcws.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;


@Component
public class WsHandler implements WebSocketHandler {

    private static final Logger logger = LoggerFactory.getLogger(WsHandler.class);

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.send(session.receive().map(WebSocketMessage::getPayloadAsText)
                .doOnNext(txt -> logger.info("IN [{}]: {}", session.getId(), txt))
                .map(txt -> "Echo: " + txt)
                .map(session::textMessage)
                );
    }
}
