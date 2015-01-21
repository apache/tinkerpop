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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnionStep<S, E> extends ComputerAwareStep<S, E> implements TraversalHolder {

    private static final Child[] CHILD_OPERATIONS = new Child[]{Child.SET_HOLDER, Child.MERGE_IN_SIDE_EFFECTS, Child.SET_SIDE_EFFECTS};

    private List<Traversal<S, E>> unionTraversals;

    public UnionStep(final Traversal traversal, final Traversal<S, E>... unionTraversals) {
        super(traversal);
        this.unionTraversals = Arrays.asList(unionTraversals);
        for(final Traversal<S,E> child : this.unionTraversals) {
            child.asAdmin().addStep(new EndStep(child));
            this.executeTraversalOperations(child, CHILD_OPERATIONS);
        }

    }

    @Override
    protected Iterator<Traverser<E>> standardAlgorithm() {
        while (true) {
            for (final Traversal<S, E> union : this.unionTraversals) {
                if (union.hasNext()) return TraversalHelper.getEnd(union.asAdmin());
            }
            final Traverser.Admin<S> start = this.starts.next();
            this.unionTraversals.forEach(union -> union.asAdmin().addStart(start.split()));
        }
    }

    @Override
    protected Iterator<Traverser<E>> computerAlgorithm() {
        final List<Traverser<E>> ends = new ArrayList<>();
        while (ends.isEmpty()) {
            final Traverser.Admin<S> start = this.starts.next();
            for (final Traversal<S, E> union : this.unionTraversals) {
                final Traverser.Admin<S> unionSplit = start.split();
                unionSplit.setStepId(TraversalHelper.getStart(union.asAdmin()).getId());
                ends.add((Traverser) unionSplit);
            }
        }
        return ends.iterator();
    }

    @Override
    public List<Traversal<S, E>> getGlobalTraversals() {
        return Collections.unmodifiableList(this.unionTraversals);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.unionTraversals);
    }

    @Override
    public UnionStep<S, E> clone() throws CloneNotSupportedException {
        final UnionStep<S, E> clone = (UnionStep<S, E>) super.clone();
        clone.unionTraversals = new ArrayList<>();
        for (final Traversal<S, E> unionTraversals : this.unionTraversals) {
            final Traversal<S,E> unionTraversalClone = unionTraversals.clone();
            clone.unionTraversals.add(unionTraversalClone);
            clone.executeTraversalOperations(unionTraversalClone, CHILD_OPERATIONS);
        }
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.resetTraversals();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return TraversalHolder.super.getRequirements();
    }
}
