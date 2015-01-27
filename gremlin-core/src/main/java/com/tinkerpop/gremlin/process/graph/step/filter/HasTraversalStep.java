package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalLambda;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HasTraversalStep<S> extends FilterStep<S> implements TraversalHolder {

    // TODO: cloning --- traversalLambda and FilterStep.predicate (they are the same!)

    private TraversalLambda<S, ?> traversalLambda;

    public HasTraversalStep(final Traversal traversal, final Traversal<S, ?> hasTraversal) {
        super(traversal);
        this.traversalLambda = new TraversalLambda<>(hasTraversal);
        this.setPredicate(this.traversalLambda);
        this.executeTraversalOperations(this.traversalLambda.getTraversal(), TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.traversalLambda);
    }

    @Override
    public List<Traversal<S, ?>> getLocalTraversals() {
        return Collections.singletonList(this.traversalLambda.getTraversal());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.traversalLambda.getRequirements();
    }

    @Override
    public HasTraversalStep<S> clone() throws CloneNotSupportedException {
        final HasTraversalStep<S> clone = (HasTraversalStep<S>) super.clone();
        clone.traversalLambda = this.traversalLambda.clone();
        clone.executeTraversalOperations(clone.traversalLambda.getTraversal(), TYPICAL_LOCAL_OPERATIONS);
        clone.setPredicate(clone.traversalLambda);
        return clone;
    }
}
