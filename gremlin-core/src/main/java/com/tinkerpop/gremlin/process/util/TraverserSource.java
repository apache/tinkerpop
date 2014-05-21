package com.tinkerpop.gremlin.process.util;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraverserSource {

    public void generateTraverserIterator(final boolean trackPaths);

    public void clear();
}
