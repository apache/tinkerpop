package com.tinkerpop.gremlin.server;

/**
 * Result codes for Gremlin Server responses.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum ResultCode {
    SUCCESS(200),
    FAIL(400),
    FAIL_MALFORMED_REQUEST(401),
    FAIL_INVALID_REQUEST_ARGUMENTS(402);


    private final int value;
    private ResultCode(final int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
