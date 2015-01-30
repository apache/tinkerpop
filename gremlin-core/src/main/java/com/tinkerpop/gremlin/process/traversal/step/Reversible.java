package com.tinkerpop.gremlin.process.traversal.step;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Reversible {

    public default void reverse() {
    }
}
