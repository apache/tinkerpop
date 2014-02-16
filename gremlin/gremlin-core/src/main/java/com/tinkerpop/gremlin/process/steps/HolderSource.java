package com.tinkerpop.gremlin.process.steps;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface HolderSource {

    public void generateHolderIterator(final boolean trackPaths);
}
