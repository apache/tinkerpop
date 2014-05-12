package com.tinkerpop.gremlin.driver.exception;

import com.tinkerpop.gremlin.driver.message.ResultCode;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResponseException extends Exception {
    private ResultCode resultCode;

    public ResponseException(final ResultCode resultCode, final String serverMessage) {
        super(serverMessage);
        this.resultCode = resultCode;
    }

    public ResultCode getResultCode() {
        return resultCode;
    }
}