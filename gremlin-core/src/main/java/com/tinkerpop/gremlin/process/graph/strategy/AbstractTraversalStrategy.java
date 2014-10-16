package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractTraversalStrategy implements TraversalStrategy {

    @Override
    public String toString() {
        return StringFactory.traversalStrategyString(this);
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return this.getClass().equals(object.getClass());
    }
}
