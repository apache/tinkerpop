package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnionStep<S, E> extends AbstractStep<S, E> implements TraversalHolder<S, E> {

    private List<Traversal<S, E>> traversals;

    @SafeVarargs
    public UnionStep(final Traversal traversal, final Traversal<S, E>... unionTraversals) {
        super(traversal);
        this.traversals = Arrays.asList(unionTraversals);
        this.traversals.forEach(union -> {
            union.asAdmin().setStrategies(this.getTraversal().asAdmin().getStrategies());
            union.asAdmin().setTraversalHolder(this);
        });
    }

    @Override
    protected Traverser<E> processNextStart() {
        while (true) {
            for (final Traversal<S, E> union : this.traversals) {
                if (union.hasNext()) return TraversalHelper.getEnd(union).next();
            }
            final Traverser.Admin<S> start = this.starts.next();
            this.traversals.forEach(union -> union.asAdmin().addStart(start.split()));
        }
    }

    @Override
    public List<Traversal<S, E>> getTraversals() {
        return Collections.unmodifiableList(this.traversals);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.traversals);
    }

    @Override
    public UnionStep<S, E> clone() throws CloneNotSupportedException {
        final UnionStep<S, E> clone = (UnionStep<S, E>) super.clone();
        clone.traversals = new ArrayList<>();
        for (final Traversal<S, E> union : this.traversals) {
            final Traversal<S, E> unionClone = union.clone();
            clone.traversals.add(unionClone);
            unionClone.asAdmin().setTraversalHolder(clone);
        }
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = new HashSet<>();
        for (final Traversal<S, E> union : this.traversals) {
            requirements.addAll(TraversalHelper.getRequirements(union));
        }
        return requirements;
    }
}
