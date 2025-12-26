package org.jpsoft.tfcws.ws.msg.error;

public enum ErrorCode {
    BAD_JSON("Invalid JSON"),
    BAD_MESSAGE("Invalid message"),
    NOT_JOINED("Send JOIN first"),
    JOIN_TIMEOUT("JOIN not received in time"),
    BAD_MOVE("Invalid MOVE payload");

    private final String message;

    ErrorCode(String defaultMessage) {
        this.message = defaultMessage;
    }

    public String defaultMessage() {
        return message;

    }
}
