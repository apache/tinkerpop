package com.tinkerpop.gremlin.driver.ser;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SerializationException extends Exception {
    public SerializationException(final Throwable t) {
        super(t);
    }
}
