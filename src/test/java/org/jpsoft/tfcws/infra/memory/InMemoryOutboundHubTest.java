package org.jpsoft.tfcws.infra.memory;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class InMemoryOutboundHubTest {

    @Test
    void outbound_emits_messages_in_order() {
        var hub = new InMemoryOutboundHub();
        var sessionId = "s1";

        hub.register(sessionId);

        StepVerifier.create(hub.outboundMessages(sessionId).take(2))
                .then(() -> {
                    hub.sendMessage(sessionId, "a");
                    hub.sendMessage(sessionId, "b");
                })
                .expectNext("a")
                .expectNext("b")
                .verifyComplete();
    }

    @Test
    void unregister_completes_stream() {
        var hub = new InMemoryOutboundHub();
        var sessionId = "s1";

        hub.register(sessionId);

        StepVerifier.create(hub.outboundMessages(sessionId))
                .then(() -> hub.unregister(sessionId))
                .verifyComplete();
    }
}
