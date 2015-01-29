package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.AbstractStep;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AndStep<S> extends AbstractStep<S, S> implements TraversalHolder {

    private List<Traversal<S, ?>> andTraversals;

    public AndStep(final Traversal traversal, final Traversal<S, ?>... andTraversals) {
        super(traversal);
        this.andTraversals = Arrays.asList(andTraversals);
        for (final Traversal<S, ?> andTraversal : this.andTraversals) {
            this.executeTraversalOperations(andTraversal, TYPICAL_LOCAL_OPERATIONS);
        }
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        while (true) {
            final Traverser.Admin<S> start = this.starts.next();
            boolean found = false;
            for (final Traversal<S, ?> traversal : this.andTraversals) {
                traversal.asAdmin().addStart(start.split());
                found = traversal.hasNext();
                traversal.asAdmin().reset();
                if (!found) break;
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
        return this.andTraversals;
    }

    @Override
    public AndStep<S> clone() throws CloneNotSupportedException {
        final AndStep<S> clone = (AndStep<S>) super.clone();
        clone.andTraversals = new ArrayList<>();
        for (final Traversal<S, ?> andTraversal : this.andTraversals) {
            final Traversal<S, ?> clonedAndTraversal = andTraversal.clone();
            clone.andTraversals.add(clonedAndTraversal);
            clone.executeTraversalOperations(clonedAndTraversal, TYPICAL_LOCAL_OPERATIONS);
        }
        return clone;
    }
}
