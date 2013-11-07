package com.tinkerpop.blueprints.mailbox;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphSystemMemory {

    public void incrIteration();

    public void setRuntime(final long runtime);
}
