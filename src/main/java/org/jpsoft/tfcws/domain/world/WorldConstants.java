package org.jpsoft.tfcws.domain.world;

/**
 * Constantes globales relacionadas con la división del mundo en _chunks_
 * y el cálculo del área de interés (AOI).
 *
 * <p>Clase no instanciable que agrupa valores estáticos usados por la lógica
 * de geometría del mundo (por ejemplo en {@code ChunkGeometry}).</p>
 *
 * @author Jose Luis García Pelayo
 * @since 1.0
 */
public final class WorldConstants {
    private WorldConstants() {
        // Clase de utilidades;
    }

    public static final int CHUNK_SIZE = 32;
    public static final int AOI = 1;
}
