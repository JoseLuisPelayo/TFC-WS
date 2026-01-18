package org.jpsoft.tfcws.config;

import org.jpsoft.tfcws.adapter.ws.WsHandler;
import org.jpsoft.tfcws.app.port.*;
import org.jpsoft.tfcws.domain.actor.SessionState;
import org.jpsoft.tfcws.infra.memory.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class WsConfig {

    /**
     * Crea y configura un {@link HandlerMapping} para los endpoints WebSocket.
     *
     * <p>Comportamiento detallado:</p>
     * <ul>
     *   <li>Se crea un {@link HashMap} que asocia la ruta \"/ws/game\" con la instancia de {@link WsHandler} inyectada.</li>
     *   <li>Se instancia un {@link SimpleUrlHandlerMapping}, se establece su orden a {@code 1}
     *       (prioridad alta frente a mappings con orden mayor) y se le asigna el mapa de rutas.</li>
     *   <li>El {@code SimpleUrlHandlerMapping} devolverá el {@link WebSocketHandler} correspondiente
     *       cuando llegue una conexión que coincida con la URL mapeada; el framework se encarga de
     *       negociar el upgrade a WebSocket y delegar la sesión al handler.</li>
     * </ul>
     *
     * <p>Parámetros y retorno:</p>
     * @param handler implementación de {@link WebSocketHandler} (en este proyecto, {@link WsHandler})
     *                inyectada por Spring. Debe gestionar la lógica de mensajes, control de sesiones y cierre.
     * @return {@link HandlerMapping} configurado (específicamente un {@link SimpleUrlHandlerMapping})
     *         que enruta la ruta \"/ws/game\" al handler provisto.
     */

    @Bean
    public HandlerMapping handlerMapping(WsHandler handler) {
        Map<String, WebSocketHandler> map = new HashMap<>();
        map.put("/ws/game", handler);

        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setOrder(1);
        mapping.setUrlMap(map);
        return mapping;
    }

    @Bean
    public WebSocketHandlerAdapter handlerAdapter() {
        return new WebSocketHandlerAdapter();
    }

    @Bean
    public EntityStateStore sessionStateStore() {
        return new InMemoryEntityStateStore();
    }

    @Bean
    public Presence presence() {
        return new InMemoryPresence();
    }

    @Bean
    public SessionRegistry sessionRegistry() {
        return new InMemorySessionRegistry();
    }

    @Bean
    public OutboundHub outboundHub() {
        return new InMemoryOutboundHub();
    }

    @Bean
    public SessionStateStore sessionStateStoreStore() {
        return new InMemorySessionStateStore();
    }


}
