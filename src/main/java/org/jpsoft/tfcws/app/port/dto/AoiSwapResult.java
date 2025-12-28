package org.jpsoft.tfcws.app.port.dto;

import org.jpsoft.tfcws.domain.world.ChunkCoord;

import java.util.Set;

public record AoiSwapResult(Set<ChunkCoord> enteredZones, Set<ChunkCoord> exitedZones) {
}
