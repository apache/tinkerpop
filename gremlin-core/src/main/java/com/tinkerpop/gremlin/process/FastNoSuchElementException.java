package com.tinkerpop.gremlin.process;

import java.util.NoSuchElementException;

/**
 * Retrieve a singleton, fast {@link java.util.NoSuchElementException} without a stack trace.
 */
public final class FastNoSuchElementException extends NoSuchElementException {

    private static final long serialVersionUID = 2303108654138257697L;
    private static final FastNoSuchElementException INSTANCE = new FastNoSuchElementException();

    private FastNoSuchElementException() {
    }

    /**
     * Retrieve a singleton, fast {@link NoSuchElementException} without a stack trace.
     */
    public static NoSuchElementException instance() {
        return INSTANCE;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

}
