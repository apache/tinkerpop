package com.tinkerpop.gremlin.process;

import java.util.Collections;
import java.util.Set;

/**
 * A {@link TraversalStrategy} defines a particular atomic operation for mutating a {@link Traversal} prior to its evaluation.
 * Traversal strategies are typically used for optimizing a traversal for the particular underlying graph engine.
 * Traversal strategies implement {@link Comparable} and thus are sorted to determine their evaluation order.
 * A TraversalStrategy should not have a public constructor as they should not maintain state between applications.
 * Make use of a singleton instance() object to reduce object creation on the JVM.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TraversalStrategy {

    // A TraversalStrategy should not have a public constructor
    // Make use of a singleton instance() object to reduce object creation on the JVM
    // Moreover they are stateless objects.

    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine traversalEngine);

    public default Set<Class<? extends TraversalStrategy>> applyPrior() {
        return Collections.emptySet();
    }

    public default Set<Class<? extends TraversalStrategy>> applyPost() {
        return Collections.emptySet();
    }

}
