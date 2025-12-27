package org.jpsoft.tfcws.infra.memory;

import org.jpsoft.tfcws.app.port.SessionRegistry;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
     * Implementación en memoria del registro de sesiones/zonas.
     *
     * <p>
     * Mantiene dos <b>vistas</b> de los datos:
     * <ul>
     *   <li><b>sessionsByZones</b>: para cada zona (ChunkCoord) el conjunto de sessionId suscritos.</li>
     *   <li><b>zonesBySessions</b>: para cada sessionId el conjunto de zonas a las que está suscrito.</li>
     * </ul>
     *
     * <h3>Objetivo</h3>
     * <p>
     * Garantizar que ambas vistas se actualizan de forma <b>coherente</b> (cambios atomicos o en bloque)
     * en operaciones compuestas (alta/baja), incluso con múltiples hilos (WebSocket) accediendo a la vez.
     * </p>
     *
     * <h3>Concurrencia</h3>
     * <ul>
     *   <li>Se usan <b>ConcurrentHashMap</b> y <b>sets concurrentes</b> para permitir lecturas concurrentes
     *   sin bloquear toda la estructura.</li>
     *   <li>Para operaciones <b>compuestas</b> (que tocan ambas vistas) se usa un <b>lock por sesión</b>
     *   (ver {@link #lockFor(String)}) que reduce la contención y evita interbloqueos (no se bloquea por zona).</li>
     * </ul>
     *
     * <h3>Atomicidad (visión conceptual)</h3>
     * <p>
     * La operación de alta/baja se considera <i>atómica</i> a nivel lógico: dentro de una región crítica
     * (el lock de la sesión) se actualiza <b>zone→sessions</b> y <b>session→zones</b> como una única unidad.
     * Así, un lector externo no verá estados intermedios inconsistentes por mucho tiempo.
     * </p>
     *
     * <h3>Buenas prácticas aplicadas</h3>
     * <ul>
     *   <li><b>Snapshot defensivo:</b> al dar de alta se crea un set propio concurrente con las zonas recibidas,
     *   evitando que el llamador modifique después ese conjunto y corrompa el estado interno.</li>
     *   <li><b>Remove condicional:</b> al vaciar una zona se usa remove(key, value) para no borrar si otro hilo
     *   ha cambiado la referencia del set (seguro ante carreras).</li>
     *   <li><b>Getters seguros:</b> devuelven copias inmutables (Set.copyOf) para no exponer estructuras internas.</li>
     * </ul>
     */
@Component
public class InMemoryRegistry implements SessionRegistry {

    private static final Logger log = LoggerFactory.getLogger(InMemoryRegistry.class);

    /**
     * Vista: zona -> sesiones.
     *
     * <p>Concurrente para permitir múltiples lecturas/escrituras.
     * Los valores son sets concurrentes (creados con newKeySet()) para admitir add/remove sin bloquear globalmente.</p>
     */
    private final ConcurrentMap<ChunkCoord, Set<String>> sessionsByZone;
    /**
     * Vista: sesión -> zonas.
     *
     * <p>Se guarda un <b>snapshot</b> propio concurrente por cada sesión para evitar que el caller modifique después
     * el conjunto que nos pasó y comprometa la consistencia interna.</p>
     */
    private final ConcurrentMap<String, Set<ChunkCoord>> zonesBySessions;
    /**
     * Locks por sesión.
     *
     * <p>Un lock por sessionId permite que distintas sesiones realicen altas/bajas en paralelo sin bloquearse entre sí.
     * Evitamos locks por zona para no pedir múltiples locks en diferentes órdenes (riesgo de deadlock).
     * Cada operación compuesta (alta/baja) toma <b>solo</b> el lock de su sesión.</p>
     */
    private final ConcurrentMap<String, ReentrantLock> sessionLocks;


    public InMemoryRegistry() {
        this.sessionsByZone = new ConcurrentHashMap<>();
        this.zonesBySessions = new ConcurrentHashMap<>();
        this.sessionLocks = new ConcurrentHashMap<>();
    }

    /**
     * Alta de suscripción inicial: asocia una sesión a un conjunto de zonas (AOI inicial).
     *
     * <p><b>Reglas clave:</b></p>
     * <ul>
     *   <li>Validación temprana de argumentos (sessionId no nulo/blank, zonas no vacías).</li>
     *   <li>Se entra en sección crítica (lock por sesión) para actualizar <b>ambas vistas</b> como una unidad.</li>
     *   <li>Se crea un <b>snapshot concurrente</b> con las zonas recibidas (copia defensiva).</li>
     *   <li>Primero se actualiza <b>zone→sessions</b> (computeIfAbsent + add), y <b>después</b> se publica
     *   <b>session→zones</b> con el snapshot (reduce ventanas donde un lector vea la sesión sin todas sus zonas).</li>
     * </ul>
     *
     * @param sessionId identificador de sesión (String)
     * @param zones conjunto de zonas (máx. 9 en AOI r=1) que se van a asociar a la sesión
     * @throws IllegalArgumentException si sessionId es nulo/blank o zones es nulo/vacío
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
     * <p><b>Pasos:</b></p>
     * <ol>
     *   <li>Validación de sessionId.</li>
     *   <li>Lock por sesión (región crítica).</li>
     *   <li>Se toma un <b>snapshot</b> de sus zonas actuales removiendo la entrada en session→zones.</li>
     *   <li>Para cada zona del snapshot, se quita la sesión del set; si el set queda vacío,
     *   se intenta borrar la entrada con <b>remove(key,value)</b> (seguro ante carreras).</li>
     *   <li>Se libera el lock y, opcionalmente, se elimina de {@code sessionLocks} para liberar memoria.</li>
     * </ol>
     *
     * <p>Esta secuencia mantiene la invariante de consistencia entre las dos vistas y evita NPEs en carreras.</p>
     *
     * @param sessionId identificador de sesión (String)
     * @throws IllegalArgumentException si sessionId es nulo/blank
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

            log.info("event=session_unsubscribed sessionId={} zones={}", sessionId, (zones == null ) ? Set.of() : zones);
        } finally {
            lock.unlock();
        }

        sessionLocks.remove(sessionId);
    }

    /**
     * Consulta las zonas de una sesión.
     *
     * <p>Devuelve una <b>copia inmutable</b> (Set.copyOf) para no exponer
     * la estructura interna y evitar modificaciones externas.</p>
     *
     * @param sessionId identificador de sesión
     * @return conjunto inmutable de zonas asociadas a la sesión, o Set.of() si no hay
     */
    @Override
    public Set<ChunkCoord> getZonesBySessionId(String sessionId) {
        Set<ChunkCoord> zones = zonesBySessions.get(sessionId);
        return (zones == null || zones.isEmpty()) ? Set.of() : Set.copyOf(zones);
    }

    /**
     * Consulta las sesiones suscritas a una zona.
     *
     * <p>Devuelve una <b>copia inmutable</b> (Set.copyOf) para no exponer
     * la estructura interna y evitar modificaciones externas.</p>
     *
     * @param zone zona consultada
     * @return conjunto inmutable de sessionId en esa zona, o Set.of() si no hay
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
     * Se usa computeIfAbsent para crear el lock solo la primera vez (operación atómica y thread-safe).
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
