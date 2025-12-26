package org.jpsoft.tfcws.ws.codec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jpsoft.tfcws.ws.msg.Envelope;
import org.jpsoft.tfcws.ws.msg.MsgType;
import org.jpsoft.tfcws.ws.msg.SnapShotZonePayload;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class MsgCodec {
    private final ObjectMapper mapper;

    public MsgCodec(ObjectMapper mapper) {
        this.mapper = mapper.copy().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public Envelope parseEnvelope(String json) throws IOException {
        return mapper.readValue(json, Envelope.class);
    }

    public String encodeSnapShotZone(SnapShotZonePayload payload) {
        ObjectNode root = mapper.createObjectNode();
        root.put("type", MsgType.SNAPSHOT_ZONE.name());
        root.set("payload", mapper.valueToTree(payload));

        try {
            return mapper.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error encoding SnapShotZonePayload", e);
        }
    }


}
