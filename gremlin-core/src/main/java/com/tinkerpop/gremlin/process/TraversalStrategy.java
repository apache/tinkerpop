package com.tinkerpop.gremlin.process;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalStrategy extends Serializable, Comparable<TraversalStrategy> {

    // A TraversalStrategy should not have a public constructor
    // Make use of a singleton instance() object to reduce object creation on the JVM

    public void apply(final Traversal traversal);

    public interface NoDependencies extends TraversalStrategy {
        public default int compareTo(final TraversalStrategy traversalStrategy) {
            return traversalStrategy instanceof NoDependencies ? -1 : -1 * traversalStrategy.compareTo(this);
        }
    }

}
