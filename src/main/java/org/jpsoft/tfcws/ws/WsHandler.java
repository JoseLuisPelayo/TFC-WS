package org.jpsoft.tfcws.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * WebSocket handler reactivo basado en Spring WebFlux.
 *
 * <p>Funciona como un eco: todo lo que recibe por el socket lo reenvía al
 * cliente prefijado con "Echo: ". Además, registra trazas del ciclo de vida
 * de la sesión (inicio, mensajes recibidos y fin).
 */
@Component
public class WsHandler implements WebSocketHandler {

    private static final Logger logger = LoggerFactory.getLogger(WsHandler.class);

    /**
     * Maneja una sesión WebSocket construyendo dos streams:
     *
     * <ul>
     *   <li><b>inbound</b> (lectura): stream de cadenas con los textos recibidos.</li>
     *   <li><b>outbound</b> (escritura): stream de mensajes que se envían al cliente.</li>
     * </ul>
     *
     * <p>El {@code Mono<Void>} devuelto completa cuando el envío de {@code outbound}
     * termina (por cierre remoto, error o cancelación). No se almacenan mensajes en
     * memoria más allá de lo que dicta el backpressure de WebFlux.
     */
    @Override
    public Mono<Void> handle(WebSocketSession session) {
        String id = session.getId();
        String address = String.valueOf(session.getHandshakeInfo().getRemoteAddress());

        // Stream INBOUND: leer mensajes entrantes del cliente.
        Flux<String> inbound = session.receive()
                // doOnSubscribe: se ejecuta cuando alguien se suscribe al stream
                // (en la práctica, cuando comenzamos a consumir los mensajes entrantes).
                .doOnSubscribe(sub -> logger.info("WebSocket session started: id={}, address={}", id, address))
                // map: transformar cada WebSocketMessage al texto contenido en su payload.
                .map(WebSocketMessage::getPayloadAsText)
                // doOnNext: callback por cada elemento recibido (sin modificar el flujo).
                // Aquí registramos que llegó un mensaje (no se imprime el contenido por diseño actual).
                .doOnNext(msg -> logger.info("WebSocket session received: id={}, address={}", id, address))
                // doFinally: callback terminal que se ejecuta al terminar el flujo por
                // onComplete, onError o cancelación (cierre de la sesión).
                .doFinally(s -> logger.info("WebSocket session ended: id={}, address={}", id, address));

        // Stream OUTBOUND: construir los mensajes a enviar de vuelta al cliente a partir de INBOUND.
        Flux<WebSocketMessage> outbound = inbound
                // map: prefijar cada texto recibido con "Echo: " (lógica de eco).
                .map(txt -> "Echo: " + txt)
                // map: envolver el texto en un WebSocketMessage propio de la sesión.
                .map(session::textMessage);

        return session.send(outbound);

    }
}
