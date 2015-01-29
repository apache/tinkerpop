package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalLambda;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HasTraversalStep<S> extends AbstractStep<S, S> implements TraversalHolder {

    private TraversalLambda<S, ?> traversalLambda;

    public HasTraversalStep(final Traversal traversal, final Traversal<S, ?> hasTraversal) {
        super(traversal);
        this.traversalLambda = new TraversalLambda<>(hasTraversal);
        this.executeTraversalOperations(this.traversalLambda.getTraversal(), TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        while (true) {
            final Traverser<S> start = this.starts.next();
            if (this.traversalLambda.test(start))
                return start;
        }
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
        return clone;
    }
}
