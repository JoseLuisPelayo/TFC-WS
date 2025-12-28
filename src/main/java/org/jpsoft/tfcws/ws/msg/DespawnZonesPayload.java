package org.jpsoft.tfcws.ws.msg;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.jpsoft.tfcws.domain.world.ChunkCoord;

import java.util.Set;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class DespawnZonesPayload {
    private Set<ChunkCoord> zones;
}
