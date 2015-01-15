package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraverserSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnionStep<S, E> extends AbstractStep<S, E> implements TraversalHolder<S, E>, EngineDependent {

    private List<Traversal<S, E>> traversals;

    private TraverserSet<E> graphComputerQueue;
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
    public void onEngine(final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER)) {
            this.graphComputerQueue = new TraverserSet<>();
            this.futureSetByChild = true;
        }
    }

    @Override
    protected Traverser<E> processNextStart() {
        return null == this.graphComputerQueue ? this.standardAlgorithm() : this.computerAlgorithm();
    }

    private Traverser<E> standardAlgorithm() {
        while (true) {
            for (final Traversal<S, E> union : this.traversals) {
                if (union.hasNext()) return TraversalHelper.getEnd(union).next();
            }
            final Traverser.Admin<S> start = this.starts.next();
            this.traversals.forEach(union -> union.asAdmin().addStart(start.split()));
        }
    }

    private Traverser<E> computerAlgorithm() {
        if (this.first) {
            this.first = false;
            for (final Traversal<S, E> union : this.traversals) {
                TraversalHelper.getEnd(union).setNextStep(this.getNextStep());
            }
        }
        while (true) {
            if (!this.graphComputerQueue.isEmpty())
                return this.graphComputerQueue.remove();
            final Traverser.Admin<S> start = this.starts.next();
            for (final Traversal<S, E> union : this.traversals) {
                final Traverser.Admin<S> unionSplit = start.split();
                unionSplit.setFuture(TraversalHelper.getStart(union).getLabel());
                this.graphComputerQueue.add((Traverser.Admin) unionSplit);

            }
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
        if (null != this.graphComputerQueue)
            clone.graphComputerQueue = new TraverserSet<>();
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
