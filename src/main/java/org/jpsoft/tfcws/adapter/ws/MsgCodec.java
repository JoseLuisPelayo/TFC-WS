package org.jpsoft.tfcws.adapter.ws;

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

    public <T> T parsePayload(Envelope envelope, Class<T> payloadClass) throws JsonProcessingException {
            return mapper.treeToValue(envelope.getPayload(), payloadClass);
    }

    public String encode(MsgType type, Object payload) {
        ObjectNode root = mapper.createObjectNode();
        root.put("type", type.name());
        root.set("payload", mapper.valueToTree(payload));

        try {
            return mapper.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error encoding payload of type: " + type, e);
        }
    }

    public String encodeSnapShotZone(SnapShotZonePayload payload) {
        return encode(MsgType.SNAPSHOT_ZONE, payload);
    }


}
