package org.jpsoft.tfcws.app.port;

import org.jpsoft.tfcws.app.port.dto.AoiSwapResult;
import org.jpsoft.tfcws.domain.world.ChunkCoord;

import java.util.Set;

/**
 * Registro de sesiones por zona (AOI) usado por los componentes de suscripción.
 *
 * <p>Define operaciones para:
 * - asociar una sesión identificada por {@code sessionId} a un conjunto de zonas
 *   representadas por {@link ChunkCoord},
 * - eliminar la sesión del registro,
 * - consultar las zonas asociadas a una sesión,
 * - consultar las sesiones asociadas a una zona.</p>
 *
 * <p>Contrato y expectativas:
 * <ul>
 *   <li>Los parámetros {@code sessionId} y {@code zones} se esperan no nulos;
 *       las implementaciones pueden lanzar {@link NullPointerException} si se
 *       les pasa {@code null}.</li>
 * </ul>
 *
 * @author Jose Luis García Pelayo
 * @since 1.0
 */
public interface SessionRegistry {
    void addSessionsToZones(String sessionId, Set<ChunkCoord> zones);
    void removeSession(String sessionId);
    AoiSwapResult swapAoiZones(String sessionId, ChunkCoord newChunkCoord);
    Set<ChunkCoord> getZonesBySessionId(String sessionId);
    Set<String> getSessionsByZone(ChunkCoord zone);
}
