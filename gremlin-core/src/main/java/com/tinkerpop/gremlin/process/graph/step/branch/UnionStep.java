package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalRing;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnionStep<S, E> extends AbstractStep<S, E> implements TraversalHolder<S, E> {

    private TraversalRing<S, E> traversalRing;

    @SafeVarargs
    public UnionStep(final Traversal traversal, final Traversal<S, E>... branchTraversals) {
        super(traversal);
        this.traversalRing = new TraversalRing<>(branchTraversals);
        this.traversalRing.forEach(branch -> branch.asAdmin().setStrategies(this.getTraversal().asAdmin().getStrategies()));
    }

    @Override
    protected Traverser<E> processNextStart() {
        while (true) {
            int counter = 0;
            while (counter++ < this.traversalRing.size()) {
                final Traversal<S, E> branch = this.traversalRing.next();
                if (branch.hasNext()) return TraversalHelper.getEnd(branch).next();
            }
            final Traverser.Admin<S> start = this.starts.next();
            this.traversalRing.forEach(branch -> branch.asAdmin().addStart(start.split()));
        }
    }

    @Override
    public Collection<Traversal<S, E>> getTraversals() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, Arrays.asList(this.traversalRing.getTraversals()));
    }

    @Override
    public UnionStep<S, E> clone() throws CloneNotSupportedException {
        final UnionStep<S, E> clone = (UnionStep<S, E>) super.clone();
        clone.traversalRing = this.traversalRing.clone();
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = new HashSet<>();
        for (final Traversal<?, ?> unionTraversal : this.traversalRing.getTraversals()) {
            requirements.addAll(TraversalHelper.getRequirements(unionTraversal));
        }
        if (requirements.contains(TraverserRequirement.SINGLE_LOOP))
            requirements.add(TraverserRequirement.NESTED_LOOP);
        return requirements;
    }
}
