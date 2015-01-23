package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHasNextPredicate;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HasTraversalStep<S> extends FilterStep<S> implements TraversalHolder {

    // TODO: cloning --- traversalPredicate and FilterStep.predicate (they are the same!)

    private TraversalHasNextPredicate<S, ?> traversalPredicate;

    public HasTraversalStep(final Traversal traversal, final Traversal<S, ?> hasTraversal) {
        super(traversal);
        this.traversalPredicate = new TraversalHasNextPredicate<>(hasTraversal);
        this.setPredicate(this.traversalPredicate);
        this.executeTraversalOperations(this.traversalPredicate.getTraversal(), Child.SET_HOLDER);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.traversalPredicate);
    }

    @Override
    public List<Traversal<S, ?>> getLocalTraversals() {
        return Collections.singletonList(this.traversalPredicate.getTraversal());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return TraversalHelper.getRequirements(this.traversalPredicate.getTraversal().asAdmin());
    }

    @Override
    public HasTraversalStep<S> clone() throws CloneNotSupportedException {
        final HasTraversalStep<S> clone = (HasTraversalStep<S>) super.clone();
        clone.traversalPredicate = this.traversalPredicate.clone();
        clone.setPredicate(clone.traversalPredicate);
        clone.executeTraversalOperations(clone.traversalPredicate.getTraversal(), Child.SET_HOLDER);
        return clone;
    }
}
