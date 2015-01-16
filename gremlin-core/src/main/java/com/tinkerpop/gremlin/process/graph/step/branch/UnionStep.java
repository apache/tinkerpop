package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.util.ComputerAwareStep;
import com.tinkerpop.gremlin.process.graph.strategy.SideEffectCapStrategy;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnionStep<S, E> extends ComputerAwareStep<S, E> implements TraversalHolder<S, E> {

    private static final Child[] CHILD_OPERATIONS = new Child[]{Child.SET_HOLDER, Child.SET_STRATEGIES, Child.MERGE_IN_SIDE_EFFECTS, Child.SET_SIDE_EFFECTS};

    private List<Traversal<S, E>> traversals;

    public UnionStep(final Traversal traversal, final Traversal<S, E>... unionTraversals) {
        super(traversal);
        this.traversals = Arrays.asList(unionTraversals);
        this.executeTraversalOperations(CHILD_OPERATIONS);
    }

    @Override
    public TraversalStrategies getChildStrategies() {
        return TraversalHolder.super.getChildStrategies().removeStrategies(SideEffectCapStrategy.class); // no auto cap();
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
        final List<Traverser<E>> ends = new ArrayList<>();
        while (ends.isEmpty()) {
            final Traverser.Admin<S> start = this.starts.next();
            for (final Traversal<S, E> union : this.traversals) {
                final Traverser.Admin<S> unionSplit = start.split();
                unionSplit.setFutureId(TraversalHelper.getStart(union).getId());
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
            clone.traversals.add(union.clone());
        }
        clone.executeTraversalOperations(CHILD_OPERATIONS);
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.resetTraversals();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getTraversalRequirements();
    }


}
