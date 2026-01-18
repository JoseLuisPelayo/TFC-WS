package org.jpsoft.tfcws.adapter.ws.msg;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Set;
import java.util.UUID;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class DespawnPlayerPayload {
    private Set<UUID> entityIds;
}
