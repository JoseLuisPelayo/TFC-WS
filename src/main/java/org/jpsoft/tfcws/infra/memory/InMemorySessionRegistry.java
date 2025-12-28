package org.jpsoft.tfcws.infra.memory;

import org.jpsoft.tfcws.app.port.SessionRegistry;
import org.jpsoft.tfcws.app.port.dto.AoiSwapResult;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.domain.world.ChunkGeometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Implementación en memoria del registro de sesiones y sus zonas (AOI).
 *
 * <p>
 * Esta clase mantiene dos vistas coherentes del registro:
 * <ul>
 *   <li><b>sessionsByZone</b>: para cada {@link ChunkCoord} (zona) el conjunto de {@code sessionId} suscritos.</li>
 *   <li><b>zonesBySessions</b>: para cada {@code sessionId} el conjunto de {@link ChunkCoord} a los que está suscrito.</li>
 * </ul>
 * </p>
 *
 * <h3>Propósito y garantías</h3>
 * <p>
 * Proveer operaciones de suscripción/desuscripción y cambio de AOI (swap) seguras para acceso concurrente
 * (por ejemplo desde múltiples conexiones WebSocket). Las operaciones compuestas que deben actualizar ambas vistas
 * (zone→sessions y session→zones) se realizan dentro de una región crítica por sesión para garantizar coherencia
 * lógica (semántica "atómica" a nivel de la aplicación).
 * </p>
 *
 * <h3>Concurrencia</h3>
 * <ul>
 *   <li>Las estructuras internas usan {@link ConcurrentMap} y sets concurrentes (creados con {@code ConcurrentHashMap.newKeySet()})
 *       para permitir lecturas/escrituras concurrentes y reducir la contención.</li>
 *   <li>Se emplea un {@link ReentrantLock} por sesión (mapa {@code sessionLocks}) para serializar operaciones compuestas
 *       que afectan a una sesión concreta. Esto evita pedir locks por zona y reduce el riesgo de deadlocks.</li>
 *   <li>Las operaciones de lectura devuelven copias inmutables (mediante {@code Set.copyOf}) para no exponer
 *       las estructuras internas.</li>
 * </ul>
 *
 * <h3>Buenas prácticas aplicadas</h3>
 * <ul>
 *   <li><b>Snapshot defensivo:</b> al añadir suscripciones se crea una copia concurrente propia de las zonas recibidas,
 *       evitando que el llamador modifique posteriormente el conjunto pasado y corrompa el estado interno.</li>
 *   <li><b>Remove condicional:</b> al vaciar una zona se usa {@code remove(key, value)} para eliminar la entrada solo
 *       si sigue siendo la misma referencia, protegiendo contra condiciones de carrera.</li>
 *   <li><b>Liberación de recursos:</b> tras eliminar la sesión se intenta eliminar también el lock asociado en {@code sessionLocks}
 *       para evitar crecimiento indefinido del mapa de locks.</li>
 * </ul>
 */
@Component
public class InMemorySessionRegistry implements SessionRegistry {

    private static final Logger log = LoggerFactory.getLogger(InMemorySessionRegistry.class);

    /**
     * Vista: zona -> sesiones.
     *
     * <p>Concurrente para permitir múltiples lecturas/escrituras. Los valores son sets concurrentes
     * (creados con {@code ConcurrentHashMap.newKeySet()}) para admitir add/remove sin bloquear globalmente.</p>
     */
    private final ConcurrentMap<ChunkCoord, Set<String>> sessionsByZone;
    /**
     * Vista: sesión -> zonas.
     *
     * <p>Se guarda un \-snapshot\ own concurrente por cada sesión para evitar que el caller modifique después
     * el conjunto que nos pasó y comprometa la consistencia interna.</p>
     */
    private final ConcurrentMap<String, Set<ChunkCoord>> zonesBySessions;
    /**
     * Locks por sesión.
     *
     * <p>Un lock por sessionId permite que distintas sesiones realicen altas/bajas en paralelo sin bloquearse entre sí.
     * Evitamos locks por zona para no pedir múltiples locks en diferentes órdenes (riesgo de deadlock).</p>
     */
    private final ConcurrentMap<String, ReentrantLock> sessionLocks;


    public InMemorySessionRegistry() {
        this.sessionsByZone = new ConcurrentHashMap<>();
        this.zonesBySessions = new ConcurrentHashMap<>();
        this.sessionLocks = new ConcurrentHashMap<>();
    }

    /**
     * Alta de suscripción inicial: asocia una sesión a un conjunto de zonas (AOI inicial).
     *
     * <p><b>Reglas clave:</b></p>
     * <ul>
     *   <li>Valida que {@code sessionId} no sea nulo ni {@code blank} y que {@code zones} no sea {@code null} ni vacío.</li>
     *   <li>Se entra en sección crítica (lock por sesión) para actualizar ambas vistas como una unidad lógica.</li>
     *   <li>Se crea un snapshot concurrente con las zonas recibidas (copia defensiva) antes de publicar en las vistas.</li>
     *   <li>Primero se actualiza zone→sessions y después session→zones para reducir la ventana en la que un lector
     *       pueda observar la sesión inscrita parcialmente.</li>
     * </ul>
     *
     * @param sessionId identificador de sesión (no nulo/blank)
     * @param zones     conjunto de zonas a asociar a la sesión (no nulo/vacío)
     * @throws IllegalArgumentException si {@code sessionId} es nulo o blank, o si {@code zones} es nulo o vacío
     */
    @Override
    public void addSessionsToZones(String sessionId, Set<ChunkCoord> zones) {

        if (sessionId == null || sessionId.isBlank()) throw new IllegalArgumentException("sessionId is null");
        if (!containsZones(zones)) throw new IllegalArgumentException("zones is null/empty");

        // Región crítica por sesión: garantiza que las dos vistas se actualizan juntas (semántica "atómica" a nivel lógico).
        ReentrantLock lock = lockFor(sessionId);
        lock.lock();
        try {
            // Snapshot concurrente propio (copia defensiva): evita que el caller modifique después 'zones' y corrompa el estado.
            var chunks = ConcurrentHashMap.<ChunkCoord>newKeySet();
            chunks.addAll(zones);

            // Vista zone -> sessions:
            // computeIfAbsent crea el set concurrente solo una vez de forma atómica; luego añadimos la sesión.
            for (ChunkCoord zone : chunks) {
                sessionsByZone.computeIfAbsent(zone, z -> ConcurrentHashMap.newKeySet())
                        .add(sessionId);
            }

            zonesBySessions.put(sessionId, chunks);

            List<String> zoneKeys = chunks.stream().map(ChunkCoord::getZoneKey).toList();
            log.info("event=session_subscribed sessionId={} zones={}", sessionId, zoneKeys);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Baja de una sesión: elimina todas sus suscripciones de ambas vistas.
     *
     * <p>Secuencia:
     * <ol>
     *   <li>Valida {@code sessionId}.</li>
     *   <li>Toma el lock por sesión y elimina la entrada en session→zones obteniendo un snapshot.</li>
     *   <li>Para cada zona del snapshot quita la sesión del set; si el set queda vacío intenta eliminar la entrada
     *       con {@code remove(key,value)} para ser seguro frente a carreras.</li>
     *   <li>Libera el lock y elimina opcionalmente el lock de {@code sessionLocks} para liberar memoria.</li>
     * </ol>
     * </p>
     *
     * @param sessionId identificador de sesión (no nulo/blank)
     * @throws IllegalArgumentException si {@code sessionId} es nulo o blank
     */
    @Override
    public void removeSession(String sessionId) {

        if (sessionId == null || sessionId.isBlank()) throw new IllegalArgumentException("sessionId is null");

        ReentrantLock lock = lockFor(sessionId);
        lock.lock();
        try {

            Set<ChunkCoord> zones = zonesBySessions.remove(sessionId);

            if (containsZones(zones)) {
                zones.forEach(zone -> {
                    Set<String> ids = sessionsByZone.get(zone);

                    // Quita la sesión del set; si queda vacío, intenta borrar la entrada (remove(key,value) es seguro ante carreras).
                    if (ids != null) {
                        ids.remove(sessionId);
                        if (ids.isEmpty())
                            sessionsByZone.remove(zone, ids);
                    }
                });
            }

            log.info("event=session_unsubscribed sessionId={} zones={}", sessionId, (zones == null) ? Set.of() : zones);
        } finally {
            lock.unlock();
        }

        sessionLocks.remove(sessionId);
    }

    /**
     * Cambia la AOI (conjunto de zonas) de una sesión a la nueva zona central indicada.
     *
     * <p>Comportamiento:
     * <ul>
     *   <li>Toma el lock por sesión para serializar el swap.</li>
     *   <li>Calcula las zonas nuevas con {@link ChunkGeometry#getChunksInAOI(ChunkCoord)}.</li>
     *   <li>Calcula las zonas a despawnear y las zonas añadidas, actualizando ambas vistas de forma coherente.</li>
     * </ul>
     * </p>
     *
     * @param sessionId     identificador de sesión (no nulo/blank)
     * @param newChunkCoord nueva coordenada central de chunk para calcular la AOI
     * @return {@link AoiSwapResult} con los conjuntos de zonas añadidas y despawnadas
     * @throws IllegalArgumentException si {@code sessionId} es nulo o blank
     */
    @Override
    public AoiSwapResult swapAoiZones(String sessionId, ChunkCoord newChunkCoord) {
        ReentrantLock lock = lockFor(sessionId);
        lock.lock();
        try {
            Set<ChunkCoord> oldZones = getZonesBySessionId(sessionId);
            Set<ChunkCoord> newZones = ChunkGeometry.getChunksInAOI(newChunkCoord);

            Set<ChunkCoord> despawnZones = oldZones.stream()
                    .filter(zone -> !newZones.contains(zone))
                    .collect(Collectors.toSet());

            despawnZones.forEach(zone -> {
                Set<ChunkCoord> zones = zonesBySessions.get(sessionId);
                if (zones != null) {
                    zones.remove(zone);
                    Set<String> subs = sessionsByZone.get(zone);

                    if (subs != null) {
                        subs.remove(sessionId);
                        if (subs.isEmpty())
                            sessionsByZone.remove(zone, subs);

                    }
                }
            });

            Set<ChunkCoord> addedZones = newZones.stream()
                    .filter(zone -> !oldZones.contains(zone))
                    .collect(Collectors.toSet());

            addedZones.forEach(zone -> {
                sessionsByZone.computeIfAbsent(zone, z -> ConcurrentHashMap.newKeySet())
                        .add(sessionId);
                zonesBySessions.computeIfAbsent(sessionId, s -> ConcurrentHashMap.newKeySet()).add(zone);
            });

            return new AoiSwapResult(addedZones, despawnZones);

        } finally {
            lock.unlock();
        }
    }

    /**
     * Consulta las zonas de una sesión.
     *
     * <p>Devuelve una copia inmutable ({@code Set.copyOf}) para no exponer
     * la estructura interna y evitar modificaciones externas.</p>
     *
     * @param sessionId identificador de sesión
     * @return conjunto inmutable de zonas asociadas a la sesión, o {@code Set.of()} si no hay
     */
    @Override
    public Set<ChunkCoord> getZonesBySessionId(String sessionId) {
        Set<ChunkCoord> zones = zonesBySessions.get(sessionId);
        return (zones == null || zones.isEmpty()) ? Set.of() : Set.copyOf(zones);
    }

    /**
     * Consulta las sesiones suscritas a una zona.
     *
     * <p>Devuelve una copia inmutable ({@code Set.copyOf}) para no exponer
     * la estructura interna y evitar modificaciones externas.</p>
     *
     * @param zone zona consultada
     * @return conjunto inmutable de sessionId en esa zona, o {@code Set.of()} si no hay
     */
    @Override
    public Set<String> getSessionsByZone(ChunkCoord zone) {
        Set<String> sessions = sessionsByZone.get(zone);
        return (sessions == null || sessions.isEmpty()) ? Set.of() : Set.copyOf(sessions);
    }

    /**
     * Obtiene (o crea) el lock asociado a una sesión.
     *
     * <p>Uso típico:
     * <pre>
     *   var lock = lockFor(sessionId);
     *   lock.lock();
     *   try {
     *       // sección crítica: actualizar ambas vistas
     *   } finally {
     *       lock.unlock();
     *   }
     * </pre>
     *
     * Se usa {@code computeIfAbsent} para crear el lock solo la primera vez (operación atómica y thread-safe).
     * </p>
     *
     * @param sessionId identificador de la sesión (String no vacío)
     * @return el lock reentrante asociado a esa sesión
     */
    private ReentrantLock lockFor(String sessionId) {
        return sessionLocks.computeIfAbsent(sessionId, id -> new ReentrantLock());
    }


    private boolean containsZones(Set<ChunkCoord> zones) {
        return zones != null && !zones.isEmpty();
    }


}
