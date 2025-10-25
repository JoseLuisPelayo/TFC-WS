package org.jpsoft.tfcws.domain.world;

import org.jpsoft.tfcws.domain.spatial.Position;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilidades geométricas para calcular coordenadas de chunks y seleccionar
 * chunks dentro del área de interés (AOI) en un mundo 2D.
 *
 * <p>Clase final con métodos estáticos; no debe instanciarse.</p>
 *
 * @author Jose Luis García Pelayo
 * @since 1.0
 */
public final class ChunkGeometry {

    private ChunkGeometry() {
        // Clase de utilidades; no se instancia.
    }

    /**
     * Convierte una posición en coordenadas del mundo a las coordenadas de chunk
     * correspondientes.
     *
     * <p>Se divide cada componente de la posición por {@code WorldConstants.CHUNK_SIZE}
     * y se aplica {@link Math#floor} para obtener el índice entero de chunk en cada eje.</p>
     *
     * @param position posición en el mundo (coordenadas continuas)
     * @return coordenadas de chunk que contienen la posición
     */
    public static ChunkCoord posToChunk(Position position) {
        int cx = (int) Math.floor(position.x() / WorldConstants.CHUNK_SIZE);
        int cy = (int) Math.floor(position.y() / WorldConstants.CHUNK_SIZE);

        return new ChunkCoord(cx, cy);
    }

    /**
     * Obtiene la lista de chunks que están dentro del área de interés (AOI)
     * centrada en el chunk especificado.
     *
     * <p>El AOI se interpreta como un radio en número de chunks definido por
     * {@code WorldConstants.AOI}. Se recorren los índices X desde
     * {@code center.cx() - AOI} hasta {@code center.cx() + AOI} y, para cada X,
     * los índices Y en el mismo rango. El resultado es una lista con todos los chunks
     * que entran en el área de interes.</p>
     *
     * @param center coordenada de chunk que actúa como centro del AOI
     * @return lista de {@code ChunkCoord} dentro del AOI
     */
    public static List<ChunkCoord> getChunksInAOI(ChunkCoord center) {
        List<ChunkCoord> chunks = new ArrayList<>();
        int startX = center.cx() - WorldConstants.AOI;
        int endX = center.cx() + WorldConstants.AOI;
        int startY = center.cy() - WorldConstants.AOI;
        int endY = center.cy() + WorldConstants.AOI;

        for (int cx = startX; cx <= endX; cx++) {
            for (int cy = startY; cy <= endY; cy++) {
                chunks.add(new ChunkCoord(cx, cy));
            }
        }
        return chunks;
    }

}
