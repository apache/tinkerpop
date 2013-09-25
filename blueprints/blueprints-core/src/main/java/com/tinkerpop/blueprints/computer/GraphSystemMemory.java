package com.tinkerpop.blueprints.computer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphSystemMemory extends GraphMemory {

    public void incrIteration();

    public void setRuntime(final long runtime);
}
