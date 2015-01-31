package com.tinkerpop.gremlin.groovy.plugin;

/**
 * A mapper {@link Exception} to be thrown when there are problems with processing a command given to a
 * {@link com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor}.  The message provided to the exception will
 * be displayed to the user in the Console.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RemoteException extends Exception {
    public RemoteException(final String message) {
        super(message);
    }

    public RemoteException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public RemoteException(final Throwable cause) {
        super(cause);
    }
}
