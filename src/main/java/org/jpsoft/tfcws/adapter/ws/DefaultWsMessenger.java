package org.jpsoft.tfcws.app.messaging;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jpsoft.tfcws.adapter.ws.MsgCodec;
import org.jpsoft.tfcws.adapter.ws.msg.MsgType;
import org.jpsoft.tfcws.app.port.OutboundHub;
import org.jpsoft.tfcws.app.port.SessionRegistry;
import org.jpsoft.tfcws.app.port.WsMessenger;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * DefaultWsMessenger
 * <p>
 * Pequeña capa de “mensajería de salida” para que los flows NO repitan:
 * - codec.encode(...)
 * - outboundHub.sendMessage(...)
 * - sessionRegistry.getSessionsByZone(...)+loop
 * <p>
 * Idea:
 * - Los flows orquestan (validación, estado, AOI swap, etc.)
 * - Este componente se encarga de "cómo" mandar mensajes (encode + push al hub).
 * <p>
 * Importante:
 * - No toca WebSocketSession (eso es del WsHandler).
 * - Solo produce Strings JSON y las empuja al OutboundHub (buzón por sesión).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DefaultWsMessenger implements WsMessenger {

    /**
     * Convierte (MsgType + payload) -> JSON {"type": "...", "payload": {...}}
     */
    private final MsgCodec codec;

    /**
     * Buzón por sesión: el WsHandler está suscrito a outboundMessages(sessionId)
     * y manda al socket todo lo que entre aquí.
     */
    private final OutboundHub outboundHub;

    /**
     * Necesario para broadcasts por zona:
     * zona -> set(sessionIds suscritos)
     */
    private final SessionRegistry sessionRegistry;

    /**
     * Enviar un mensaje SOLO a una sesión concreta.
     * <p>
     * Uso típico:
     * messenger.sendTo(sessionId, MsgType.ERROR, new ErrorPayload(...));
     */
    @Override
    public void sendTo(String sessionId, MsgType type, Object payload) {
        // 1) Codificamos una vez (String JSON)
        String json = codec.encode(type, payload);

        // 2) Empujamos al buzón de esa sesión
        boolean ok = outboundHub.sendMessage(sessionId, json);

        // 3) Si falla, lo registramos (no lanzamos excepción aquí para no tumbar el flow)
        if (!ok) {
            log.warn("ws_send_failed sessionId={} type={}", sessionId, type);
        }
    }

    /**
     * Broadcast a todos los watchers SUSCRITOS a UNA zona concreta.
     * <p>
     * OJO:
     * - Esto NO es “AOI completo”, solo una zona.
     * - Si quieres AOI, usa broadcastToZones(...) con un set de zonas.
     */
    @Override
    public void broadcastToZone(ChunkCoord zone, MsgType type, Object payload) {
        // Codificamos una sola vez y reutilizamos el JSON para todos los watchers
        String json = codec.encode(type, payload);

        // Obtenemos la lista de sesiones suscritas a esa zona (copia inmutable en tu registry)
        sessionRegistry.getSessionsByZone(zone)
                .forEach(watcherId -> {
                    boolean ok = outboundHub.sendMessage(watcherId, json);
                    if (!ok) {
                        log.warn("ws_broadcast_failed zone={} watcherId={} type={}",
                                zone.getZoneKey(), watcherId, type);
                    }
                });
    }

    /**
     * Broadcast a varias zonas (AOI, por ejemplo).
     * <p>
     * Nota:
     * - Si una sesión está suscrita a 2 zonas del set, recibiría DUPLICADO.
     * - Para evitar duplicados, habría que hacer dedupe con un Set<String> de destinatarios.
     * (lo dejamos simple por ahora; tú decides cuándo lo necesitas).
     */
    @Override
    public void broadcastToZones(Set<ChunkCoord> zones, MsgType type, Object payload) {
        String json = codec.encode(type, payload);

        zones.forEach(zone -> sessionRegistry.getSessionsByZone(zone)
                .forEach(watcherId -> {
                    boolean ok = outboundHub.sendMessage(watcherId, json);
                    if (!ok) {
                        log.warn("ws_broadcast_failed zone={} watcherId={} type={}",
                                zone.getZoneKey(), watcherId, type);
                    }
                }));
    }
}
