package org.jpsoft.tfcws.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.jpsoft.tfcws.app.subscription.SubscriptionService;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.infra.memory.InMemoryPresence;
import org.jpsoft.tfcws.ws.codec.MsgCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Set;

/**
 * Flujo que se ejecuta cuando se establece una conexión WebSocket y se espera
 * un mensaje inicial de tipo "join" con la posición del cliente.
 *
 * <p>Responsabilidades principales:
 * <ul>
 *   <li>Leer el primer mensaje entrante del cliente (se asume que es un "join").</li>
 *   <li>Parsear ese mensaje para obtener una {@link Position} válida.</li>
 *   <li>Llamar a {@link SubscriptionService#suscribeInitialZones} para suscribir al cliente
 *       a las zonas (chunks) iniciales en función de su posición.</li>
 *   <li>Construir y devolver un {@link WebSocketMessage} JSON informando de las zonas suscritas
 *       (tipo de mensaje: {@code SUBSCRIBED}).</li>
 *   <li>En caso de error o timeout en el mensaje "join", usar una posición por defecto (0,0)
 *       y continuar con la suscripción.</li>
 * </ul>
 * <p>
 * Diseño y notas:
 * <ul>
 *   <li>Esta clase delega la lógica concreta de suscripción a {@link SubscriptionService}.</li>
 *   <li>Los métodos que parsean y construyen mensajes devuelven {@link Mono} para integrarse
 *       en cadenas reactivas sin bloquear.</li>
 *   <li>Hay un TODO para extraer los parsers a una clase separada si se desea mejorar la
 *       separación de responsabilidades.</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class OnConnectFlow {

    /**
     * Servicio encargado de gestionar las suscripciones a zonas.
     */
    private final SubscriptionService subscriptionService;
    /**
     * Gestor de presencia en memoria.
     */
    private final InMemoryPresence presence;
    /**
     * Codec para convertir objetos a mensajes WebSocket.
     */
    private final MsgCodec codec;

    /**
     * Mapper JSON usado para parsear y crear mensajes JSON de salida.
     */
    private final ObjectMapper objectMapper;

    private static final Logger log = LoggerFactory.getLogger(OnConnectFlow.class);

    /**
     * Tiempo máximo que se espera por el mensaje "join" inicial recibido desde el cliente.
     * Si no llega en este tiempo, se usa una posición por defecto (0,0).
     */
    private Duration joinTimeout = Duration.ofSeconds(5);

    /**
     * Ejecuta el flujo de conexión para una sesión WebSocket.
     *
     * <p>Comportamiento general:
     * <ol>
     *   <li>Obtiene el primer mensaje disponible del {@code inbound} usando {@code inbound.next()}.</li>
     *   <li>Aplica un timeout de {@link #joinTimeout}; si expira, se usa la posición por defecto.</li>
     *   <li>Convierte el payload de texto a {@link Position} con {@link #parseJoinErrorToPosition}.</li>
     *   <li>Si no hay mensaje (flux completado sin elementos) o si ocurre un error
     *       (timeout / parseo), se aplica un fallback a la posición (0,0).</li>
     *   <li>Con la posición resultante, construye el mensaje de suscripción y lo devuelve.
     *       El resultado es un {@link Mono}&lt;{@link WebSocketMessage}&gt; que puede ser enviado
     *       inmediatamente por el manejador WebSocket.</li>
     * </ol>
     *
     * <p>Notas sobre operadores usados en la cadena reactiva:
     * <ul>
     *   <li>{@code inbound.next()} transforma el flujo {@link Flux} en un {@link Mono}
     *       que emite sólo el primer elemento disponible o completa si no hay ninguno.</li>
     *   <li>{@code timeout(joinTimeout)} lanza un error si el primer elemento tarda más de
     *       {@code joinTimeout} en llegar; este error es manejado por {@code onErrorResume}.</li>
     *   <li>{@code map(...)} transforma el {@link WebSocketMessage} en su representación textual.</li>
     *   <li>{@code flatMap(this::parseJoinErrorToPosition)} encadena una operación asíncrona
     *       que puede devolver un error (por eso retorna {@link Mono}).</li>
     *   <li>{@code switchIfEmpty(...)} cubre el caso en el que el {@link Flux} entrante
     *       se completa sin emitir elementos (cliente no envió mensaje alguno).</li>
     *   <li>{@code onErrorResume(...)} captura errores (timeout, parseo inválido, etc.) y
     *       aplica un fallback controlado para continuar el flujo sin romper la conexión.
     * </ul>
     *
     * @param session objeto de sesión WebSocket (usado para obtener id y dirección)
     * @param inbound flujo de mensajes entrantes desde el cliente
     * @return un {@link Mono} que emite un {@link WebSocketMessage} con la confirmación de suscripción
     */
    public Flux<WebSocketMessage> run(WebSocketSession session, Flux<WebSocketMessage> inbound) {
        final String sessionId = session.getId();
        final InetSocketAddress remoteAddress = session.getHandshakeInfo().getRemoteAddress();

        // Primero: obtener el primer mensaje del cliente (si existe). Si tarda más de joinTimeout,
        // se considera timeout y se cae al onErrorResume.
        Mono<Position> positionMono = inbound.next().timeout(joinTimeout)
                // map: convertir el WebSocketMessage en su payload textual
                .map(WebSocketMessage::getPayloadAsText)
                // flatMap: parsear el JSON de "join" a Position o devolver Mono.error si es inválido
                .flatMap(this::parseJoinErrorToPosition)
                // Si el inbound se completa sin elementos (cliente no envió nada), usar fallback (0,0)
                .switchIfEmpty(Mono.defer(() -> {
                    log.error("Join_no_message_received -> fallback_position to (0,0) for sessionId={}, remoteAddress={}",
                            sessionId, remoteAddress);
                    return Mono.just(new Position(0.0, 0.0));
                }))
                // Manejo de errores: timeout o parseo inválido -> fallback (0,0) y log
                .onErrorResume(ex -> {
                    log.error("Join_timeout_or_invalid_message -> fallback_position to (0,0) for sessionId={}, remoteAddress={} cause: {}",
                            sessionId, remoteAddress, ex.getMessage());
                    return Mono.just(new Position(0.0, 0.0));
                });

        Mono<Set<ChunkCoord>> zonesMono = positionMono.map(pos -> subscriptionService.suscribeInitialZones(sessionId, pos));


        Flux<WebSocketMessage> snapshots = zonesMono.flatMapMany(
                        zones -> Flux.fromIterable(presence.buildSnapShotZone(sessionId, zones)))
                .map(codec::encodeSnapShotZone)
                .map(session::textMessage);

        Mono<WebSocketMessage> suscribedMessage = positionMono.flatMap(pos -> buildSuscriptionMessage(session, pos));
        
        return Flux.concat(suscribedMessage, snapshots);
    }

    //TODO Sacar los parsers a otra clase

    /**
     * Parsea un JSON esperado con forma: { "type": "join", "payload": { "x": <number>, "y": <number> } }
     *
     * <p>Comportamiento:
     * <ul>
     *   <li>Si el JSON no tiene campo "type" igual a "join" (case-insensitive), se falla.</li>
     *   <li>Si falta "payload" o los campos "x"/"y" dentro de payload, se falla.</li>
     *   <li>Si "x" o "y" no son numéricos, se falla.</li>
     *   <li>Si hay errores de parseo JSON, se transforman en {@link IllegalArgumentException}
     *       y se devuelven como {@link Mono#error(Throwable)} para que el llamador los maneje.
     * </ul>
     *
     * <p>Importante: este método devuelve {@link Mono}&lt;{@link Position}&gt; porque se integra
     * en cadenas reactivas donde se espera un Mono. Si el input es válido, se retorna
     * {@code Mono.just(new Position(x,y))}; en caso de error se retorna {@code Mono.error(...)}.
     *
     * @param json payload textual recibido del cliente
     * @return Mono que emite la {@link Position} parseada, o error si el JSON es inválido
     */
    private Mono<Position> parseJoinErrorToPosition(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);

            JsonNode typeNode = root.get("type");
            JsonNode payloadNode = root.get("payload");

            if (typeNode == null || !typeNode.asText().equalsIgnoreCase("join"))
                throw new IllegalArgumentException("Expected type=JOIN");

            if (payloadNode == null || payloadNode.get("x") == null || payloadNode.get("y") == null)
                throw new IllegalArgumentException("Expected payload with x and y");

            JsonNode positionXNode = payloadNode.get("x");
            JsonNode positionYNode = payloadNode.get("y");
            if (!positionXNode.isNumber() || !positionYNode.isNumber())
                throw new IllegalArgumentException("Expected numeric x and y");

            double x = positionXNode.asDouble();
            double y = positionYNode.asDouble();

            // Nota: la condición siguiente intenta detectar NaN o infinito. Si se requiere,
            // puede mejorarse para mayor claridad. Aquí se conserva la intención original.
            if (Double.isNaN(x) || Double.isNaN(y) && Double.isInfinite(x) || Double.isInfinite(y))
                throw new IllegalArgumentException("x and y must be valid numbers");

            return Mono.just(new Position(x, y));

        } catch (JsonMappingException e) {
            // JsonMappingException proviene de problemas de estructura JSON
            return Mono.error(new IllegalArgumentException("Malformed JSON structure" + e.getMessage()));
        } catch (JsonProcessingException e) {
            // Otros problemas de procesado JSON
            return Mono.error(new IllegalArgumentException("Error processing JSON: " + e.getMessage()));
        }

    }

    /**
     * Construye el mensaje de suscripción que será enviado al cliente después de determinar
     * las zonas iniciales a las que se ha suscrito.
     *
     * <p>Pasos:
     * <ol>
     *   <li>Solicita a {@link SubscriptionService#suscribeInitialZones} las zonas (keys) para el sessionId y posición.</li>
     *   <li>Empaqueta ese conjunto de zonas en un JSON con la forma:
     *       { "type": "SUBSCRIBED", "payload": { "zones": [ ... ] } }</li>
     *   <li>Convierte el JSON a String y lo envuelve en un {@link WebSocketMessage} de texto
     *       usando {@code session.textMessage(...)}.</li>
     * </ol>
     *
     * <p>Si ocurre un {@link JsonProcessingException} al construir el JSON de respuesta,
     * se registra el error y se devuelve {@link Mono#empty()} (no se envía mensaje al cliente).
     *
     * @param session  sesión WebSocket (necesaria para crear el {@link WebSocketMessage})
     * @param position posición del cliente usada para calcular las zonas iniciales
     * @return Mono que emite el {@link WebSocketMessage} listo para ser enviado, o Mono.empty()
     * si hubo un error al construir el JSON de salida
     */
    private Mono<WebSocketMessage> buildSuscriptionMessage(WebSocketSession session, Position position) {
        try {
            // Suscribe y recibe las keys (representación String) de los chunks/zones suscritos
            Set<ChunkCoord> subscribedChunks = subscriptionService.suscribeInitialZones(session.getId(), position);

            // Construcción del payload JSON: { payload: { zones: [...] } }
            ObjectNode payloadNode = objectMapper.createObjectNode();
            payloadNode.set("zones", objectMapper.valueToTree(subscribedChunks));

            ObjectNode rootNode = objectMapper.createObjectNode();
            rootNode.put("type", "SUBSCRIBED");
            rootNode.set("payload", payloadNode);

            String messageStr = objectMapper.writeValueAsString(rootNode);
            return Mono.just(session.textMessage(messageStr));

        } catch (JsonProcessingException e) {
            // Si el JSON de salida no puede generarse, simplemente logueamos y no enviamos mensaje.
            // Esto evita romper la conexión; la sesión seguirá su lifecycle y se puede enviar
            // información adicional más tarde si se desea.
            log.error("Error building subscription message for sessionId={}, position={} cause: {}",
                    session.getId(), position, e.getMessage());
            return Mono.empty();
        }
    }
}

