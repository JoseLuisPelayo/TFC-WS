package org.jpsoft.tfcws.app.subscription;

import lombok.RequiredArgsConstructor;
import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Servicio que gestiona la suscripción de sesiones a zonas AOI (Area of Interest).
 *
 * <p>Comportamiento:
 * <ul>
 *   <li>Calcula las zonas AOI relevantes para una posición dada usando {@link AoiService}.</li>
 *   <li>Registra la sesión en las zonas calculadas mediante {@link SessionRegistry}.</li>
 *   <li>Devuelve el conjunto de claves de zona a las que la sesión ha sido añadida.</li>
 * </ul>
 *
 * Este servicio es un componente de Spring y utiliza inyección de dependencias para sus colaboradores.
 */
@RequiredArgsConstructor
@Service
public class SubscriptionService {

    /**
     * Registro de sesiones por zona. Responsable de asociar una sesión a una o varias zonas.
     */
    private final SessionRegistry sessionRegistry;
    /**
     * Servicio encargado de calcular las claves de zona AOI a partir de una posición.
     */
    private final AoiService aoiService;

    /**
     * Suscribe una sesión a las zonas iniciales calculadas para la posición indicada.
     *
     * <p>Acciones realizadas:
     * <ol>
     *   <li>Obtiene las coordenadas de las zonas AOI relevantes para la posición.</li>
     *   <li>Añade la sesión a cada una de esas zonas en el {@code sessionRegistry}.</li>
     *   <li>Devuelve las claves de las zonas como un {@link Set} de {@link String}.</li>
     * </ol>
     *
     * @param sessionId identificador de la sesión que se va a suscribir
     * @param position posición del usuario/jugador que determina las zonas AOI
     * @return conjunto de claves de zona (tipo {@code String}) a las que la sesión ha sido añadida
     */
    public Set<String> suscribeInitialZones(String sessionId, Position position) {
        if (sessionId == null || sessionId.isEmpty())
            throw new IllegalArgumentException("sessionId cannot be null or empty");
        if (position == null)
            throw new IllegalArgumentException("position cannot be null");

        Set<ChunkCoord> zones = aoiService.getAoiZoneKeys(position);
        sessionRegistry.addSessionsToZones(sessionId, zones);

        return zones.stream()
                .map(ChunkCoord::getZoneKey)
                .collect(Collectors.toSet());
    }

}
