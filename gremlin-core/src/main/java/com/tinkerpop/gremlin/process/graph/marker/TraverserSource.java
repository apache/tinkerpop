package com.tinkerpop.gremlin.process.graph.marker;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraverserSource {

    public void generateTraverserIterator(final boolean trackPaths);

    public void clear();
}
