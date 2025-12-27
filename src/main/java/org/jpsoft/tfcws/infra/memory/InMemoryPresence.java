package org.jpsoft.tfcws.infra.memory;

import lombok.extern.slf4j.Slf4j;
import org.jpsoft.tfcws.app.port.Presence;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.domain.world.ChunkGeometry;
import org.jpsoft.tfcws.ws.msg.PlayerViewPayload;
import org.jpsoft.tfcws.ws.msg.SnapShotZonePayload;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Gestor en memoria de la presencia de sesiones/jugadores por zona.
 *
 * <p>Responsabilidades:
 * - Mantener la posición actual por sesión (\`positionBySession\`).
 * - Mantener para cada zona un mapa de sesiones presentes y sus posiciones
 * (\`presenceByZone\`).
 * - Serializar operaciones que afectan a una misma sesión usando un lock por sesión
 * (\`sessionLocks\`).</p>
 *
 * <p>Garantías y notas de concurrencia:
 * - Las estructuras internas usan implementaciones concurrentes para permitir
 * accesos concurrentes entre sesiones diferentes.
 * - Las operaciones que combinan múltiples estructuras por sesión (upsert/remove)
 * se realizan bajo el mismo {@link ReentrantLock} por sesión, garantizando atomicidad
 * por sesión, pero no entre sesiones diferentes.
 * - Las lecturas de snapshot son \"eventualmente consistentes\": pueden reflejar
 * un estado intermedio si hay escrituras concurrentes.</p>
 */

@Slf4j
@Component
public class InMemoryPresence implements Presence {

    /**
     * Mapa concurrente sessionId -> Position.
     * Contiene la posición actual conocida de cada sesión.
     */
    private final ConcurrentHashMap<String, Position> positionBySession = new ConcurrentHashMap<>();

    /**
     * Mapa concurrente zoneKey -> (mapa sessionId -> Position).
     * Para cada zona guarda qué sesiones están presentes y su posición en dicha zona.
     * Cada valor es un ConcurrentHashMap para permitir actualizaciones concurrentes
     * de distintas sesiones dentro de la misma zona.
     */
    private final ConcurrentHashMap<String, Map<String, Position>> presenceByZone = new ConcurrentHashMap<>();

    /**
     * Mapa concurrente sessionId -> ReentrantLock.
     * Se usa un lock por sesión para serializar operaciones que modifican las estructuras
     * relativas a esa sesión (upsert/remove).
     */
    private final ConcurrentMap<String, ReentrantLock> sessionLocks = new ConcurrentHashMap<>();

    /**
     * Inserta o actualiza la posición de una sesión y asegura su presencia en el chunk
     * correspondiente a esa posición.
     *
     * <p>Comportamiento preciso:
     * - Adquiere el {@link ReentrantLock} asociado a {@code sessionId} para serializar
     *   operaciones relativas a esa sesión (atomicidad por sesión).
     * - Actualiza {@code positionBySession} con la {@code position} proporcionada.
     * - Calcula el chunk actual con {@link ChunkGeometry#posToChunk(Position)} y
     *   garantiza que en {@code presenceByZone} exista el mapa de esa zona; añade o actualiza
     *   la entrada {@code sessionId -> position} en el mapa de esa zona.
     * - Si el conjunto {@code zones} no contiene el chunk calculado se registra una
     *   advertencia, pero la operación sigue insertando la presencia en el chunk
     *   calculado a partir de {@code position} (es decir, el método no sincroniza ni
     *   elimina presencia en otras zonas proporcionadas en {@code zones}).</p>
     *
     * <p>Garantías y notas de concurrencia:
     * - Atomicidad por sesión gracias al lock por {@code sessionId}; no hay garantía
     *   de consistencia entre sesiones concurrentes.
     * - Las estructuras internas usan tipos concurrentes (ConcurrentHashMap) para permitir
     *   accesos simultáneos entre distintas sesiones.
     * - La vista que obtengan lectores concurrentes puede reflejar un estado intermedio
     *   (no es un snapshot transaccional).</p>
     *
     * @param sessionId id de la sesión a actualizar
     * @param position  nueva posición de la sesión
     * @param zones     conjunto de zonas declarado como área de interés (AOI) del cliente;
     *                  se usa solo para validar / advertir si el chunk actual no está
     *                  dentro de esa AOI, pero no se asume que el método sincronice
     *                  presencia en todas las zonas de este conjunto.
     */
    //    Upsert es la combinación de "update" e "insert".
    public void upsertPresence(String sessionId, Position position, Set<ChunkCoord> zones) {
        ReentrantLock lock = lockFor(sessionId);
        lock.lock();

        try {
            positionBySession.put(sessionId, position);

            ChunkCoord current = ChunkGeometry.posToChunk(position);

            if (!zones.contains(current)) {
                log.warn("presence_upsert AOI does not contain current chunk sessionId={} current={} aoi={}",
                        sessionId, current, zones);
            }

            presenceByZone.computeIfAbsent(current.getZoneKey(), k -> new ConcurrentHashMap<>())
                    .put(sessionId, position);

        } finally {
            lock.unlock();
        }
    }

    /**
     * Elimina la presencia de una sesión de las estructuras en memoria.
     *
     * <p>Comportamiento preciso:
     * - Adquiere el {@link ReentrantLock} asociado a {@code sessionId} para serializar
     *   la eliminación relativa a esa sesión.
     * - Intenta eliminar la entrada en {@code positionBySession} y obtiene la última
     *   posición conocida ({@code lastPosition}).
     * - Si {@code lastPosition} existe, calcula la zona (chunk) correspondiente y
     *   elimina {@code sessionId} únicamente del mapa de esa zona.
     * - Si {@code lastPosition} es {@code null} (no hay posición conocida), se itera
     *   sobre el conjunto {@code zones} proporcionado y se remueve {@code sessionId}
     *   de cada una de esas zonas (esto cubre casos en que el estado local del caller
     *   conoce la AOI pero la posición interna fue previamente eliminada).
     * - Cuando un mapa de presencia de zona queda vacío se remueve la entrada de
     *   {@code presenceByZone} usando {@code remove(key, value)} para evitar races.</p>
     *
     * <p>Garantías y notas de concurrencia:
     * - Atomicidad por sesión mientras se mantiene el lock; otras sesiones pueden
     *   modificar sus propios datos concurrentemente.
     * - Tras liberar el lock se elimina el {@link ReentrantLock} de {@code sessionLocks}
     *   (con una pequeña ventana en la que otro hilo puede crear un nuevo lock para la
     *   misma sesión); esto evita crecimiento indefinido de la tabla de locks.</p>
     *
     * @param sessionId id de la sesión a eliminar
     * @param zones     conjunto de zonas de la AOI que el llamador considera; se usan
     *                  solo cuando no existe una posición conocida para la sesión
     *                  (es decir, {@code positionBySession} devolvió {@code null}).
     */
    public void removePresence(String sessionId, Set<ChunkCoord> zones) {
        ReentrantLock lock = lockFor(sessionId);
        lock.lock();
        try {
            Position lastPosition = positionBySession.remove(sessionId);

            if (lastPosition == null) {
                log.warn("presence_remove sessionId={} not found in positionBySession", sessionId);
            }
            if (lastPosition != null) {
                String lastZoneKey = ChunkGeometry.posToChunk(lastPosition).getZoneKey();
                Map<String, Position> zonePresence = presenceByZone.get(lastZoneKey);
                if (zonePresence != null ) {
                    zonePresence.remove(sessionId);
                    if (zonePresence.isEmpty()) {
                        presenceByZone.remove(lastZoneKey, zonePresence);
                    }
                }
            } else {
                for (ChunkCoord zone : zones) {
                    Map<String, Position> zonePresence = presenceByZone.get(zone.getZoneKey());

                    if (zonePresence != null) {
                        zonePresence.remove(sessionId);
                        if (zonePresence.isEmpty()) {
                            presenceByZone.remove(zone.getZoneKey(), zonePresence);
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }

        sessionLocks.remove(sessionId);
    }

    /**
     * Construye una lista de snapshots por zona con la vista actual de jugadores en cada zona.
     *
     * <p>Comportamiento:
     * - Para cada \`ChunkCoord\` en \`zones\` consulta \`presenceByZone\`.
     * - Si existe presencia en la zona crea una lista de {@link PlayerViewPayload} con id, nombre
     * (actualmente un placeholder \"Nombre\") y las coordenadas de posición.
     * - Devuelve una lista de {@link SnapShotZonePayload} (una por zona encontrada).</p>
     *
     * <p>Notas y consideraciones:
     * - No excluye por defecto la propia \`sessionId\`. Si se quiere omitir al jugador que
     * solicita la snapshot hay que filtrar manualmente las entradas cuyo key == sessionId.
     * - Las lecturas desde mapas concurrentes son consistentes por operación, pero la snapshot
     * puede reflejar cambios concurrentes (no es un snapshot transaccional).
     * - Se usa actualmente el literal \"Nombre\" como nombre de jugador; si existe un nombre real
     * deberá obtenerse y usarse aquí.</p>
     *
     * @param sessionId id de la sesión solicitante (no se usa para filtrar por defecto)
     * @param zones     conjunto de zonas para las que construir la snapshot
     * @return lista de snapshots por zona
     */
    public List<SnapShotZonePayload> buildSnapShotZone(String sessionId, Set<ChunkCoord> zones) {
        List<SnapShotZonePayload> snapshots = new ArrayList<>();

        zones.forEach(zone -> {
            String zoneKey = zone.getZoneKey();

            if (presenceByZone.containsKey(zoneKey)) {
                List<PlayerViewPayload> players = new ArrayList<>();
                presenceByZone.get(zoneKey).forEach((key, value) -> {
                    players.add(new PlayerViewPayload(key, "Nombre", value.x(), value.y()));
                });

                snapshots.add(new SnapShotZonePayload(zoneKey, players));
            }
        });

        return snapshots;
    }

    private ReentrantLock lockFor(String sessionId) {
        return sessionLocks.computeIfAbsent(sessionId, id -> new ReentrantLock());
    }


}
