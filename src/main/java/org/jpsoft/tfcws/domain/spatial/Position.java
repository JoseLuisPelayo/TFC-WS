package org.jpsoft.tfcws.domain.spatial;

/**
 * Representa una posición en el mundo en coordenadas continuas (precisión
 * de coma flotante) sobre los ejes X e Y.
 *
 * <p>Es un `record` inmutable con dos componentes: {@code x} y {@code y}.
 * Se utiliza como tipo ligero para operaciones espaciales (por ejemplo,
 * conversión a coordenadas de chunk en {@code ChunkGeometry}).</p>
 *
 * @param x coordenada X en unidades del mundo (double)
 * @param y coordenada Y en unidades del mundo (double)
 *
 * @author Jose Luis García Pelayo
 * @since 1.0
 */
public record Position (
    double x,
    double y
) {}
