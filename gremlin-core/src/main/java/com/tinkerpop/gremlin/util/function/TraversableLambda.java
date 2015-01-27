package com.tinkerpop.gremlin.util.function;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversableLambda<S, E> extends CloneableLambda, ResettableLambda {

    public Traversal<S, E> getTraversal();

    public default Set<TraverserRequirement> getRequirements() {
        return this.getTraversal().asAdmin().getTraverserRequirements();
    }

    @Override
    public default void reset() {
        this.getTraversal().asAdmin().reset();
    }
}
