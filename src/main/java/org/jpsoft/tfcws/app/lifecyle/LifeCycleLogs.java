package org.jpsoft.tfcws.app.lifecyle;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Componente Spring que registra mensajes en el arranque y cierre del contexto.
 *
 * <p>Escucha los eventos:
 * <ul>
 *   <li>{@link ApplicationReadyEvent} — cuando la aplicación está lista.</li>
 *   <li>{@link ContextClosedEvent} — cuando el contexto se cierra.</li>
 * </ul>
 *
 * <p>Los mensajes usan propiedades del sistema:
 * <ul>
 *   <li>`spring.application.name` — nombre de la aplicación (por defecto: "app").</li>
 *   <li>`app.env` — entorno (por defecto: "dev").</li>
 *   <li>`app.port` — puerto (por defecto: "8080").</li>
 * </ul>
 */
@Slf4j
@Component
public class LifeCycleLogs {

    /**
     * Maneja el evento de aplicación lista y escribe un log informativo.
     *
     * <p>Formato del log: {@code app_started app_name={} env={} port={}}
     * donde los valores se obtienen de propiedades del sistema con valores por defecto.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onStart() {
        String logStartTemplate = "app_started app_name={} env={} port={}";
        log.info(logStartTemplate,
                System.getProperty("spring.application.name", "app"),
                System.getProperty("app.env", "dev"),
                System.getProperty("app.port", "8080"));
    }

    /**
     * Maneja el evento de cierre del contexto y escribe un log informativo.
     *
     * <p>Formato del log: {@code app_stopped app_name={}}
     */
    @EventListener(ContextClosedEvent.class)
    public void onContextClosed() {
        log.info("app_stopped app_name={}", System.getProperty("spring.application.name", "app"));
    }
}
