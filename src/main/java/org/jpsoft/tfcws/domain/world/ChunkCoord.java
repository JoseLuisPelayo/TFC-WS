package org.jpsoft.tfcws.domain.world;

/**
 * Coordenadas de chunk en el mundo expresadas como índices de celda.
 *
 * <p>Representa una clave ligera para identificar la posición de un chunk
 * mediante dos componentes enteros: {@code cx} (coordenada X) y {@code cy}
 * (coordenada Y).</p>
 *
 * @param cx índice de celda en X
 * @param cy índice de celda en Y
 *
 * @author Jose Luis García Pelayo
 * @since 1.0
 */
public record ChunkCoord(
        int cx,
        int cy
) {

    /**
     * Devuelve la clave de zona en formato {@code "cx:cy"}.
     * <p>Útil para logs, claves de mapas o serialización simple.</p>
     * Se utiliza para identificar chunks en mapas y crear los canales de comunicación.
     *
     * @return cadena en formato {@code "cx:cy"}
     */
    public String getZoneKey() {
        return String.format("%d:%d", cx, cy);
    }

}
