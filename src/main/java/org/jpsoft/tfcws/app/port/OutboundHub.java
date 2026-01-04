package org.jpsoft.tfcws.app.port;

import reactor.core.publisher.Flux;

import java.util.Set;

/**
 * Puerto de salida (outbound) para envío y distribución de mensajes por sesión.
 *
 * <p>Implementaciones de esta interfaz gestionan el ciclo de vida de sesiones
 * (registro / desregistro), permiten enviar mensajes hacia una sesión concreta
 * y exponen un flujo reactivo de mensajes salientes por sesión.</p>
 *
 * <p>Contratos esperados:
 * <ul>
 *   <li>Las operaciones deben ser no-bloqueantes cuando sea posible.</li>
 *   <li>`sendMessage` intenta entregar el mensaje de forma asíncrona y devuelve
 *       si la entrega fue aceptada (no garantiza entrega confirmada).</li>
 *   <li>`outboundMessages` expone un {@link Flux} que emite los mensajes
 *       destinados a la sesión. El flujo debe respetar backpressure y completar
 *       o emitir error cuando la sesión se cierre o falle.</li>
 * </ul>
 * </p>
 */
public interface OutboundHub {

    /**
     * Registra una sesión identificada por {@code sessionId}.
     *
     * <p>La implementación puede preparar estructuras internas (colas, sinks,
     * suscripciones) necesarias para comenzar a enviar mensajes a la sesión.
     * Llamadas repetidas para el mismo {@code sessionId} deberán ser toleradas
     * o documentadas por la implementación concreta.</p>
     *
     * @param sessionId identificador único de la sesión; no debe ser {@code null}
     */
    void register(String sessionId);

    /**
     * Desregistra la sesión identificada por {@code sessionId}.
     *
     * <p>Se deben liberar recursos asociados (colas, sinks, listeners) y el
     * {@link Flux} devuelto por {@link #outboundMessages(String)} puede emitir
     * complete o error en función de la implementación.</p>
     *
     * @param sessionId identificador de la sesión a eliminar; no debe ser {@code null}
     */
    void unregister(String sessionId);

    /**
     * Envía un mensaje a la sesión indicada.
     *
     * <p>La operación intenta encolar o enrutar el {@code message} para su
     * entrega asíncrona. El retorno {@code true} indica que el mensaje fue
     * aceptado para entrega; {@code false} indica que no fue aceptado (por
     * ejemplo, sesión no registrada, buffer lleno, etc.). No garantiza que el
     * receptor lo haya procesado correctamente.</p>
     *
     * @param sessionId sesión destino; no debe ser {@code null}
     * @param message mensaje a enviar; no debe ser {@code null}
     * @return {@code true} si el mensaje fue aceptado para entrega, {@code false} en caso contrario
     */
    boolean sendMessage(String sessionId, String message);

    /**
     * Obtiene un flujo reactivo de mensajes salientes para la sesión indicada.
     *
     * <p>El {@link Flux} debe emitir cada mensaje destinado a la sesión en el
     * orden correspondiente y respetar las reglas de backpressure de Reactor.
     * Si la sesión no está registrada, la implementación puede devolver un
     * {@link Flux#empty()} o un {@link Flux} que emita error; esto debe quedar
     * definido por la implementación concreta.</p>
     *
     * @param sessionId sesión para la que se requieren los mensajes; no debe ser {@code null}
     * @return {@link Flux} que emite los mensajes salientes para la sesión
     */
    Flux<String> outboundMessages(String sessionId);

    /**
     * Obtiene el conjunto de IDs de sesión actualmente registradas.
     *
     * @return conjunto de IDs de sesión registradas
     */
    Set<String> sessionIds();
}
