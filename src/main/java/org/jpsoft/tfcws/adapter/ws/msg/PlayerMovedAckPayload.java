package org.jpsoft.tfcws.adapter.ws.msg;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class PlayerMovedAckPayload {
    private double x;
    private double y;
    private String chunk;
    private Long timestamp;
}
