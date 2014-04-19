package com.tinkerpop.gremlin.server.op;

import com.tinkerpop.gremlin.driver.message.ResponseMessage;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OpProcessorException extends Exception {
    private final ResponseMessage responseMessage;

    public OpProcessorException(final String message, final ResponseMessage responseMessage) {
        super(message);
        this.responseMessage = responseMessage;
    }

    public ResponseMessage getResponseMessage() {
        return this.responseMessage;
    }
}
