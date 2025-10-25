package org.jpsoft.tfcws.app.subscription;

import org.jpsoft.tfcws.domain.spatial.Position;
import org.jpsoft.tfcws.domain.world.ChunkCoord;
import org.jpsoft.tfcws.domain.world.ChunkGeometry;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * Servicio encargado de operaciones relacionadas con el área de interés (AOI)
 * y las claves de zona asociadas a los chunks.
 *
 * <p>Es un {@code @Service} que expone utilidades para obtener las
 * claves de zona de los chunks contenidos en el AOI centrado en una
 * coordenada de chunk. Las claves de zona se obtienen mediante
 * {@link ChunkCoord#getZoneKey()} en formato {@code "cx:cy"}.</p>
 *
 * @author Jose Luis García Pelayo
 * @since 1.0
 */
@Service
public final class AoiService {

    /**
     * Obtiene las claves de zona ({@code "cx:cy"}) de todos los chunks dentro
     * del área de interés (AOI) centrada en la coordenada de chunk indicada.
     *
     * <p>Este método es puro y sin efectos secundarios: calcula los chunks
     * usando {@link ChunkGeometry#getChunksInAOI(ChunkCoord)} y mapea cada
     * {@link ChunkCoord}.</p>
     *
     * @param coords coordenada de chunk que actúa como centro del AOI
     * @return conjunto inmutable de {@code ChunkCoord} que forman el AOI centrado en la posición
     */
    public Set<ChunkCoord> getAoiZoneKeys(Position coords) {
        return ChunkGeometry.getChunksInAOI(ChunkGeometry.posToChunk(coords));
    }

}
