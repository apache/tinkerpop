package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.step.AbstractStep;
import com.tinkerpop.gremlin.process.util.traversal.TraversalHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ConjunctionStep<S> extends AbstractStep<S, S> implements TraversalHolder {

    private List<Traversal<S, ?>> conjunctionTraversals;
    private final boolean isAnd;

    public ConjunctionStep(final Traversal traversal, final Traversal<S, ?>... conjunctionTraversals) {
        super(traversal);
        this.isAnd = this.getClass().equals(AndStep.class);
        this.conjunctionTraversals = Arrays.asList(conjunctionTraversals);
        for (final Traversal<S, ?> conjunctionTraversal : this.conjunctionTraversals) {
            this.executeTraversalOperations(conjunctionTraversal, TYPICAL_LOCAL_OPERATIONS);
        }
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        while (true) {
            final Traverser.Admin<S> start = this.starts.next();
            boolean found = false;
            for (final Traversal<S, ?> traversal : this.conjunctionTraversals) {
                traversal.asAdmin().addStart(start.split());
                found = traversal.hasNext();
                traversal.asAdmin().reset();
                if (this.isAnd) {
                    if (!found)
                        break;
                } else if (found)
                    break;
            }
            if (found) return start;
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return TraversalHolder.super.getRequirements();
    }

    @Override
    public List<Traversal<S, ?>> getLocalTraversals() {
        return this.conjunctionTraversals;
    }

    @Override
    public ConjunctionStep<S> clone() throws CloneNotSupportedException {
        final ConjunctionStep<S> clone = (ConjunctionStep<S>) super.clone();
        clone.conjunctionTraversals = new ArrayList<>();
        for (final Traversal<S, ?> andTraversal : this.conjunctionTraversals) {
            final Traversal<S, ?> clonedAndTraversal = andTraversal.asAdmin().clone();
            clone.conjunctionTraversals.add(clonedAndTraversal);
            clone.executeTraversalOperations(clonedAndTraversal, TYPICAL_LOCAL_OPERATIONS);
        }
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.conjunctionTraversals);
    }
}
