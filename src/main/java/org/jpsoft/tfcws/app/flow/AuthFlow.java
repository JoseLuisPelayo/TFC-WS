package org.jpsoft.tfcws.app.flow;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jpsoft.tfcws.adapter.ws.MsgCodec;
import org.jpsoft.tfcws.adapter.ws.msg.AuthOkPayload;
import org.jpsoft.tfcws.adapter.ws.msg.AuthPayload;
import org.jpsoft.tfcws.adapter.ws.msg.Envelope;
import org.jpsoft.tfcws.adapter.ws.msg.MsgType;
import org.jpsoft.tfcws.adapter.ws.msg.error.ErrorPayload;
import org.jpsoft.tfcws.app.port.SessionStateStore;
import org.jpsoft.tfcws.app.port.WsMessenger;
import org.jpsoft.tfcws.app.service.AuthService;
import org.jpsoft.tfcws.app.service.PlayerService;
import org.jpsoft.tfcws.domain.actor.SessionState;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@RequiredArgsConstructor
@Component
public class AuthFlow {

    private final AuthService authService;
    private final PlayerService playerService;
    private final SessionStateStore sessionStateStore;
    private final WsMessenger wsMessenger;
    private final MsgCodec msgCodec;

    public Mono<Void> run(Flux<Envelope> bus, WebSocketSession session) {
        return bus
                .filter(env -> env.getType() == MsgType.AUTH)
                .concatMap(env -> handleAuth(session.getId(), env))  // procesa en orden
                .then();
    }

    private Mono<Void> handleAuth(String sessionId, Envelope envelope) {
        AuthPayload payload;
        try {
            payload = msgCodec.parsePayload(envelope, AuthPayload.class);
        } catch (JsonProcessingException e) {
            wsMessenger.sendTo(
                    sessionId,
                    MsgType.ERROR,
                    new ErrorPayload("BAD_AUTH", "Invalid auth payload"));
            return Mono.empty();
        }

        return authService.loginOrRegister(payload.email(), payload.password())
                .flatMap(user -> {

                    // si existe logeo y si no existe rechazo o lo creo para pruebas

                    // Registrar sesión
                   sessionStateStore.bind(
                           sessionId,
                           SessionState.builder()
                                   .userId(user.getId())
                                   .build());

                   log.info("User {} authenticated on session {}", user.getId(),
                           sessionStateStore.getSessionState(sessionId).get().getUserId());

                    return playerService.getPlayersByUserId(user.getId())
                            .flux().collectList()
                            .doOnNext( players ->
                                    wsMessenger.sendTo(sessionId, MsgType.AUTH_OK,
                                                    new AuthOkPayload(user.getId(), players.getFirst())
                                            ))
                                            .then();
                })
                .onErrorResume(err -> {
                    wsMessenger.sendTo(
                            sessionId,
                            MsgType.ERROR,
                            new ErrorPayload("AUTH_FAILED", err.getMessage()));
                    return Mono.empty();
                });
    }

}
