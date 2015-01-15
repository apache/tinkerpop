package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.util.ComputerAwareStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnionStep<S, E> extends ComputerAwareStep<S, E> implements TraversalHolder<S, E> {

    private List<Traversal<S, E>> traversals;

    private boolean first = true;

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
    protected Iterator<Traverser<E>> standardAlgorithm() {
        while (true) {
            for (final Traversal<S, E> union : this.traversals) {
                if (union.hasNext()) return TraversalHelper.getEnd(union);
            }
            final Traverser.Admin<S> start = this.starts.next();
            this.traversals.forEach(union -> union.asAdmin().addStart(start.split()));
        }
    }

    @Override
    protected Iterator<Traverser<E>> computerAlgorithm() {
        if (this.first) {
            this.first = false;
            for (final Traversal<S, E> union : this.traversals) {
                TraversalHelper.getEnd(union).setNextStep(this.getNextStep());
            }
        }
        final List<Traverser<E>> ends = new ArrayList<>();
        while (ends.isEmpty()) {
            final Traverser.Admin<S> start = this.starts.next();
            for (final Traversal<S, E> union : this.traversals) {
                final Traverser.Admin<S> unionSplit = start.split();
                unionSplit.setFuture(TraversalHelper.getStart(union).getLabel());
                ends.add((Traverser) unionSplit);
            }
        }
        return ends.iterator();
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
        // TODO: reset children?
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
