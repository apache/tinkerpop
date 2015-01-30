package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import com.tinkerpop.gremlin.process.traversal.step.AbstractStep;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HasTraversalStep<S> extends AbstractStep<S, S> implements TraversalParent {

    private Traversal.Admin<S, ?> hasTraversal;

    public HasTraversalStep(final Traversal.Admin traversal, final Traversal.Admin<S, ?> hasTraversal) {
        super(traversal);
        this.integrateChild(this.hasTraversal = hasTraversal, TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        while (true) {
            final Traverser.Admin<S> start = this.starts.next();
            if (TraversalUtil.test(start, this.hasTraversal))
                return start;
        }
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.hasTraversal);
    }

    @Override
    public List<Traversal<S, ?>> getLocalChildren() {
        return Collections.singletonList(this.hasTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public HasTraversalStep<S> clone() throws CloneNotSupportedException {
        final HasTraversalStep<S> clone = (HasTraversalStep<S>) super.clone();
        clone.hasTraversal = clone.integrateChild(this.hasTraversal.clone(), TYPICAL_LOCAL_OPERATIONS);
        return clone;
    }
}
