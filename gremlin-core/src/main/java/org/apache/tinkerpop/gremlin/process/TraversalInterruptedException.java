package org.apache.tinkerpop.gremlin.process;

import org.apache.tinkerpop.gremlin.util.InterruptedRuntimeException;

/**
 * An unchecked exception thrown when the current thread processing a {@link Traversal} is interrupted.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalInterruptedException extends InterruptedRuntimeException {
    private final Traversal interruptedTraversal;

    public TraversalInterruptedException(final Traversal interruptedTraversal) {
        super(String.format("The %s thread received interruption notification while iterating %s - it did not complete",
                Thread.currentThread().getName(), interruptedTraversal));
        this.interruptedTraversal = interruptedTraversal;
    }

    public Traversal getInterruptedTraversal() {
        return interruptedTraversal;
    }
}
