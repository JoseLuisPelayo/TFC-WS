package org.jpsoft.tfcws.infra.memory;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jpsoft.tfcws.app.port.OutboundHub;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementación en memoria de {@link OutboundHub}.
 *
 * <p>Crea y mantiene un {@link Sinks.Many} por sesión para publicar mensajes
 * salientes de forma reactiva. Está diseñada para ser usada en memoria dentro
 * de una sola instancia de la aplicación y no persiste mensajes.</p>
 *
 * <p>Garantías y comportamiento:
 * <ul>
 *   <li>Thread-safe: utiliza {@link ConcurrentHashMap} para accesos concurrentes.</li>
 *   <li>No bloqueante: las operaciones intentan emitir de forma asíncrona usando {@link Sinks}.</li>
 *   <li>Backpressure: cada sink se crea con un buffer de tamaño {@link #MAX_QUEUE_SIZE}
 *       aplicado como comportamiento de backpressure.</li>
 *   <li>Registro/desregistro: {@link #unregister(String)} completa el flujo de la sesión
 *       llamando a {@code tryEmitComplete()} para notificar a los suscriptores.</li>
 *   <li>Entrega de mensajes: {@link #sendMessage(String, String)} devuelve {@code true}
 *       si el mensaje fue aceptado por el sink; {@code false} en caso contrario
 *       (sesión inexistente, buffer lleno, u otro fallo de emisión).</li>
 * </ul>
 * </p>
 */
@Slf4j
@RequiredArgsConstructor
public class InMemoryOutboundHub implements OutboundHub {

    /**
     * Tamaño máximo del buffer por sesión para el sink de salida.
     */
    static final int MAX_QUEUE_SIZE = 256;

    /**
     * Mapa de sessionId a su correspondiente sink de salida.
     *
     * <p>La presencia de una entrada indica que la sesión está registrada y
     * puede recibir mensajes.</p>
     */
    private final ConcurrentHashMap<String, Sinks.Many<String>> outboxes = new ConcurrentHashMap<>();

    /**
     * Registra (o prepara) la sesión identificada por {@code sessionId}.
     *
     * <p>Si ya existe un sink para la sesión, la llamada es tolerada (idempotente).
     * La implementación crea un sink multicast con buffer de tamaño {@link #MAX_QUEUE_SIZE}
     * para gestionar backpressure.</p>
     *
     * @param sessionId identificador único de la sesión; no debe ser {@code null}
     */
    @Override
    public void register(String sessionId) {
        outboxes.computeIfAbsent(sessionId,
                id -> Sinks.many().multicast().onBackpressureBuffer(MAX_QUEUE_SIZE, false));
    }

    /**
     * Desregistra la sesión e intenta completar su flujo reactivo.
     *
     * <p>Se elimina el sink del mapa y, si existía, se invoca {@code tryEmitComplete()}
     * para notificar a los suscriptores que no habrá más mensajes. Esta operación
     * libera recursos asociados a la sesión en memoria.</p>
     *
     * @param sessionId identificador de la sesión a eliminar; no debe ser {@code null}
     */
    @Override
    public void unregister(String sessionId) {
        Sinks.Many<String> sink = outboxes.remove(sessionId);
        if (sink != null)
            sink.tryEmitComplete();
    }

    /**
     * Intenta enviar un mensaje a la sesión indicada.
     *
     * <p>La operación usa {@code tryEmitNext} sobre el {@link Sinks.Many} asociado.
     * Retorna {@code true} si la emisión fue aceptada; {@code false} si no existe
     * la sesión o la emisión falló (por ejemplo buffer lleno o error de emisión).
     * No garantiza que el consumidor haya procesado el mensaje, solo que el mensaje
     * fue aceptado por el sink.</p>
     *
     * @param sessionId sesión destino; no debe ser {@code null}
     * @param message mensaje a enviar; no debe ser {@code null}
     * @return {@code true} si el mensaje fue aceptado para entrega, {@code false} en caso contrario
     */
    @Override
    public boolean sendMessage(String sessionId, String message) {
        Sinks.Many<String> sink = outboxes.get(sessionId);
        if (sink != null) {
            Sinks.EmitResult result = sink.tryEmitNext(message);
            if (result.isSuccess()) {
                return true;
            } else {
                log.warn("Failed to send message to session {}: {}", sessionId, result);
            }
        }
        return false;
    }

    /**
     * Obtiene un {@link Flux} que emite los mensajes salientes para la sesión.
     *
     * <p>Si la sesión está registrada devuelve {@code sink.asFlux()} que:
     * <ul>
     *   <li>Emite mensajes en el orden en que se publican.</li>
     *   <li>Respeta backpressure según la configuración del sink.</li>
     *   <li>Completa cuando se llama a {@link #unregister(String)} para la sesión.</li>
     * </ul>
     * Si la sesión no existe devuelve {@link Flux#empty()}.</p>
     *
     * @param sessionId sesión para la que se requieren los mensajes; no debe ser {@code null}
     * @return {@link Flux} que emite los mensajes salientes para la sesión o {@code Flux.empty()} si no registrada
     */
    @Override
    public Flux<String> outboundMessages(String sessionId) {
        Sinks.Many<String> sink = outboxes.get(sessionId);
        return sink != null ? sink.asFlux() : Flux.empty();
    }
}
