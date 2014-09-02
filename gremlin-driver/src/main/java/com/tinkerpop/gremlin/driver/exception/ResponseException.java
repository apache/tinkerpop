package com.tinkerpop.gremlin.driver.exception;

import com.tinkerpop.gremlin.driver.message.ResponseStatusCode;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResponseException extends Exception {
    private ResponseStatusCode responseStatusCode;

    public ResponseException(final ResponseStatusCode responseStatusCode, final String serverMessage) {
        super(serverMessage);
        this.responseStatusCode = responseStatusCode;
    }

    public ResponseStatusCode getResponseStatusCode() {
        return responseStatusCode;
    }
}