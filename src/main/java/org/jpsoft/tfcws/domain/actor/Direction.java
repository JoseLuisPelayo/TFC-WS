package org.jpsoft.tfcws.domain.actor;

/**
 * Direcciones cardinales usadas para orientar actores y movimientos en el mundo.
 *
 * <p>Este {@code enum} representa las cuatro direcciones básicas que pueden
 * adoptar entidades o estructuras dentro del juego: NORTH, NORTH_EAST, NORTH_WEST, EAST, WEST,
 * SOUTH_EAST, SOUTH_WEST, SOUTH.</p>
 *
 * @author Jose Luis García Pelayo
 * @since 1.0
 */
public enum Direction {
    NORTH,
    NORTH_EAST,
    EAST,
    SOUTH_EAST,
    SOUTH,
    SOUTH_WEST,
    NORTH_WEST,
    WEST
}
