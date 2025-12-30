// java
package org.jpsoft.tfcws.infra.memory;

import lombok.extern.slf4j.Slf4j;
import org.jpsoft.tfcws.app.port.Presence;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.domain.world.ChunkGeometry;
import org.jpsoft.tfcws.adapter.ws.msg.PlayerViewPayload;
import org.jpsoft.tfcws.adapter.ws.msg.SnapShotZonePayload;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Gestor en memoria de la presencia de sesiones/jugadores por zona.
 *
 * <p>
 * Responsabilidades principales:
 * <ul>
 *   <li>Mantener la posición actual por sesión en {@code positionByPlayer}.</li>
 *   <li>Mantener, por cada zona (chunk), el mapa de sesiones presentes y sus posiciones en {@code playerByChunk}.</li>
 *   <li>Serializar operaciones que afectan a una misma sesión usando un lock por sesión en {@code locksByPlayerId}.</li>
 * </ul>
 * </p>
 *
 * <p>
 * Invariantes y garantías de concurrencia:
 * <ul>
 *   <li>Las estructuras internas son concurrentes para permitir accesos paralelos de sesiones distintas.</li>
 *   <li>Operaciones que afectan a una misma sesión (upsert/remove) se serializan mediante un {@link ReentrantLock}
 *       por sesión garantizando atomicidad por sesión, sin bloquear otras sesiones.</li>
 *   <li>Las operaciones de lectura (snapshots) son eventualemente consistentes: pueden reflejar estados intermedios
 *       cuando hay escrituras concurrentes.</li>
 * </ul>
 * </p>
 */
@Slf4j
@Component
public class InMemoryPresence implements Presence {

    /**
     * Mapa concurrente sessionId -> Position.
     * Contiene la posición actual conocida de cada sesión.
     */
    private final ConcurrentHashMap<String, Position> positionByPlayer = new ConcurrentHashMap<>();

    /**
     * Mapa concurrente zoneKey -> (mapa sessionId -> Position).
     * Para cada zona guarda qué sesiones están presentes y su posición en dicha zona.
     * Cada valor es un ConcurrentHashMap para permitir actualizaciones concurrentes
     * de distintas sesiones dentro de la misma zona.
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Position>> playerByChunk = new ConcurrentHashMap<>();

    /**
     * Mapa concurrente sessionId -> ReentrantLock.
     * Se usa un lock por sesión para serializar operaciones que modifican las estructuras
     * relativas a esa sesión (upsert/remove). Los locks se eliminan cuando ya no son necesarios
     * para evitar crecimiento indefinido.
     */
    private final ConcurrentMap<String, ReentrantLock> locksByPlayerId = new ConcurrentHashMap<>();

    /**
     * Inserta o actualiza la posición de una sesión y asegura su presencia en el chunk
     * correspondiente a esa posición.
     *
     * <p>Comportamiento:
     * <ul>
     *   <li>Valida que {@code playerId} y {@code position} no sean {@code null} y que {@code playerId} no esté vacío.</li>
     *   <li>Adquiere el {@link ReentrantLock} asociado a {@code playerId} para serializar operaciones relativas a esa sesión.</li>
     *   <li>Actualiza {@code positionByPlayer} y ajusta la pertenencia en {@code playerByChunk}: elimina la entrada del chunk antiguo
     *       si cambió y añade/actualiza la entrada en el chunk actual calculado con {@link ChunkGeometry#posToChunk(Position)}.</li>
     *   <li>Si el mapa de una zona queda vacío se elimina de {@code playerByChunk} usando {@code remove(key, value)} para evitar races.</li>
     * </ul>
     * </p>
     *
     * @param playerId id de la sesión a actualizar; no puede ser {@code null} ni vacío
     * @param position nueva posición de la sesión; no puede ser {@code null}
     * @throws IllegalArgumentException si {@code playerId} es {@code null} o vacío, o si {@code position} es {@code null}
     */
    @Override
    public void upsertPresence(String playerId, Position position) {
        if (playerId == null || playerId.isEmpty()) {
            throw new IllegalArgumentException("playerId no puede ser null o vacío");
        }
        if (position == null) {
            throw new IllegalArgumentException("position no puede ser null");
        }

        ReentrantLock lock = lockFor(playerId);
        lock.lock();
        try {
            Position oldPosition = positionByPlayer.put(playerId, position);
            ChunkCoord oldChunk = null;
            ChunkCoord current = ChunkGeometry.posToChunk(position);

            if (oldPosition != null) {
                oldChunk = ChunkGeometry.posToChunk(oldPosition);
            }

            if (oldChunk != null && !oldChunk.equals(current)) {
                ConcurrentHashMap<String, Position> playersInChunk = playerByChunk.get(oldChunk.getZoneKey());
                if (playersInChunk != null) {
                    playersInChunk.remove(playerId);
                    if (playersInChunk.isEmpty()) {
                        playerByChunk.remove(oldChunk.getZoneKey(), playersInChunk);
                    }
                }
            }

            playerByChunk.computeIfAbsent(current.getZoneKey(), k -> new ConcurrentHashMap<>())
                    .put(playerId, position);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Elimina la presencia de una sesión de las estructuras en memoria.
     *
     * <p>Comportamiento:
     * <ul>
     *   <li>Valida que {@code playerId} no sea {@code null} ni vacío.</li>
     *   <li>Adquiere el {@link ReentrantLock} asociado a {@code playerId} para serializar la eliminación relativa a esa sesión.</li>
     *   <li>Elimina la entrada en {@code positionByPlayer} y, si existía una posición, remueve la presencia
     *       de {@code playerId} del chunk correspondiente.</li>
     *   <li>Si el mapa de presencia de la zona queda vacío se elimina la entrada de {@code playerByChunk} con {@code remove(key, value)}.</li>
     *   <li>Tras liberar el lock se intenta eliminar el lock de {@code locksByPlayerId} para evitar crecimiento indefinido.</li>
     * </ul>
     * </p>
     *
     * @param playerId id de la sesión a eliminar; no puede ser {@code null} ni vacío
     * @throws IllegalArgumentException si {@code playerId} es {@code null} o vacío
     */
    @Override
    public void removePresence(String playerId) {
        if (playerId == null || playerId.isEmpty()) {
            throw new IllegalArgumentException("playerId no puede ser null o vacío");
        }

        ReentrantLock lock = lockFor(playerId);
        lock.lock();
        try {
            Position lastPosition = positionByPlayer.remove(playerId);

            if (lastPosition == null) {
                log.warn("presence_remove playerId={} not found in positionByPlayer", playerId);
            }

            if (lastPosition != null) {
                String lastZoneKey = ChunkGeometry.posToChunk(lastPosition).getZoneKey();
                Map<String, Position> zonePresence = playerByChunk.get(lastZoneKey);
                if (zonePresence != null) {
                    zonePresence.remove(playerId);
                    if (zonePresence.isEmpty()) {
                        playerByChunk.remove(lastZoneKey, zonePresence);
                    }
                }
            }
        } finally {
            lock.unlock();
        }

        locksByPlayerId.remove(playerId, lock);
    }

    /**
     * Construye una lista de snapshots por zona con la vista actual de jugadores en cada zona.
     *
     * <p>Comportamiento:
     * <ul>
     *   <li>Valida que {@code playerId} no sea {@code null} ni vacío y que {@code zones} no sea {@code null}.</li>
     *   <li>Para cada {@link ChunkCoord} en {@code zones} consulta {@code playerByChunk} y, si hay presencia,
     *       crea una {@link SnapShotZonePayload} con una lista de {@link PlayerViewPayload} (id, nombre y coordenadas).</li>
     *   <li>No se excluye automáticamente la propia {@code playerId}; si se desea omitirla el llamador debe filtrar.</li>
     * </ul>
     * </p>
     *
     * @param playerId id de la sesión solicitante (no se usa para filtrar por defecto); no puede ser {@code null} ni vacío
     * @param zones conjunto de zonas para las que construir la snapshot; no puede ser {@code null}
     * @return lista de {@link SnapShotZonePayload} (nunca {@code null}, puede ser vacía)
     * @throws IllegalArgumentException si {@code playerId} es {@code null} o vacío, o si {@code zones} es {@code null}
     */
    @Override
    public List<SnapShotZonePayload> buildSnapShotZone(String playerId, Set<ChunkCoord> zones) {
        if (playerId == null || playerId.isEmpty()) {
            throw new IllegalArgumentException("playerId no puede ser null o vacío");
        }
        if (zones == null) {
            throw new IllegalArgumentException("zones no puede ser null");
        }

        List<SnapShotZonePayload> snapshots = new ArrayList<>();

        zones.forEach(zone -> {
            String zoneKey = zone.getZoneKey();

            ConcurrentHashMap<String, Position> playersInZone = playerByChunk.get(zoneKey);
            if (playersInZone != null && !playersInZone.isEmpty()) {
                List<PlayerViewPayload> players = new ArrayList<>();
                playersInZone.forEach((key, value) -> {
                    players.add(new PlayerViewPayload(key, "Nombre", value.x(), value.y()));
                });

                snapshots.add(new SnapShotZonePayload(zoneKey, players));
            }
        });

        return snapshots;
    }

    /**
     * Obtiene o crea el {@link ReentrantLock} asociado a {@code playerId}.
     *
     * <p>El lock es usado para serializar operaciones que afectan a la misma sesión.
     * Se crea bajo demanda y puede ser eliminado posteriormente desde {@link #removePresence}.</p>
     *
     * @param playerId id de la sesión; no debe ser {@code null}
     * @return lock asociado a {@code playerId}
     */
    private ReentrantLock lockFor(String playerId) {
        return locksByPlayerId.computeIfAbsent(playerId, id -> new ReentrantLock());
    }
}
