package com.tinkerpop.gremlin.process;

/**
 * A {@link TraversalStrategy} defines a particular atomic operation for mutating a {@link Traversal} prior to its evaluation.
 * Traversal strategies are typically used for optimizing a traversal for the particular underlying graph engine.
 * Traversal strategies implement {@link Comparable} and thus are sorted to determine their evaluation order.
 * If a strategy does not have any dependencies on other strategies, then implement {@link com.tinkerpop.gremlin.process.TraversalStrategy.NoDependencies}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalStrategy extends Comparable<TraversalStrategy> {

    // A TraversalStrategy should not have a public constructor
    // Make use of a singleton instance() object to reduce object creation on the JVM

    public void apply(final Traversal<?,?> traversal);

    public interface NoDependencies extends TraversalStrategy {
        @Override
        public default int compareTo(final TraversalStrategy traversalStrategy) {
            return traversalStrategy instanceof NoDependencies ? -1 : -1 * traversalStrategy.compareTo(this);
        }
    }
}
