package org.jpsoft.tfcws.adapter.ws.msg.error;

public enum ErrorCode {
    BAD_JSON("Invalid JSON"),
    BAD_MESSAGE("Invalid message"),
    NOT_JOINED("Send JOIN first"),
    JOIN_TIMEOUT("JOIN not received in time"),
    BAD_MOVE("Invalid MOVE payload"),
    BAD_STATE("Invalid session state");

    private final String message;

    ErrorCode(String defaultMessage) {
        this.message = defaultMessage;
    }

    public String defaultMessage() {
        return message;

    }
}
