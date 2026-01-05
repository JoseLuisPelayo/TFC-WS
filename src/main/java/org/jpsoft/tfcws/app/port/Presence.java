package org.jpsoft.tfcws.app.port;

import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.adapter.ws.msg.SnapShotZonePayload;

import java.util.List;
import java.util.Set;

/**
 * Contrato para la gestión de presencias de sesiones en el mundo.
 *
 * <p>
 * Implementaciones deben encargarse de mantener la ubicación (presencia)
 * asociada a una sesión identificada por {@code sessionId}, permitir su
 * eliminación y generar "snapshots" de zonas (chunks) relevantes para una
 * sesión concreta.
 * </p>
 *
 * <p>
 * Consideraciones generales:
 * <ul>
 *   <li>Los parámetros {@code sessionId}, {@code position} y {@code zones} no deben ser {@code null}.</li>
 *   <li>Los métodos pueden ser invocados concurrentemente; la implementación debe
 *       garantizar la consistencia adecuada (sin asumir sincronización externa).</li>
 *   <li>Si una sesión no existe al construir un snapshot, se puede devolver una lista vacía.</li>
 * </ul>
 * </p>
 */
public interface Presence {

    /**
     * Inserta o actualiza la presencia de la sesión indicada en la posición dada.
     *
     * <p>Comportamiento esperado:
     * <ul>
     *   <li>Si {@code sessionId} no existe, crear la entrada de presencia.</li>
     *   <li>Si ya existe, actualizar la posición asociada a la sesión.</li>
     * </ul>
     * </p>
     *
     * @param sessionId identificador único de la sesión; no puede ser {@code null} ni vacío
     * @param position  posición actual de la sesión; no puede ser {@code null}
     * @throws IllegalArgumentException si {@code sessionId} o {@code position} son {@code null} o inválidos
     */
    void upsertPresence(String sessionId, Position position);

    /**
     * Elimina la presencia asociada a la sesión indicada.
     *
     * <p>Si la sesión no existe, el método debe ser una operación de no efecto
     * (idempotente).</p>
     *
     * @param sessionId identificador único de la sesión; no puede ser {@code null} ni vacío
     * @throws IllegalArgumentException si {@code sessionId} es {@code null} o inválido
     */
    void removePresence(String sessionId);

    // Devuelve un conjunto con los IDs de las entidades presentes en la zona indicada
    Set<String> getEntitiesInZone(ChunkCoord chunkCoord);
}
