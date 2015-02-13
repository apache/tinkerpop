package org.apache.tinkerpop.gremlin.util;

/**
 * An unchecked version of the {@link java.lang.InterruptedException}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class InterruptedRuntimeException extends RuntimeException {
    public InterruptedRuntimeException() {
    }

    public InterruptedRuntimeException(final String message) {
        super(message);
    }

    public InterruptedRuntimeException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InterruptedRuntimeException(final Throwable cause) {
        super(cause);
    }
}
