package org.jpsoft.tfcws.adapter.ws.msg;

import jakarta.validation.constraints.Email;

public record AuthPayload(
        @Email
        String email,
        String password
) {
}
