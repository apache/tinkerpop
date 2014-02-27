package com.tinkerpop.gremlin.process.util;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface HolderSource {

    public void generateHolderIterator(final boolean trackPaths);
}
