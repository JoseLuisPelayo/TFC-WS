package org.jpsoft.tfcws.adapter.ws.msg;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SnapShotZonePayload {
    private String zoneId;
    private List<PlayerViewPayload> players;
}
