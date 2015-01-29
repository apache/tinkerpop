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
public final class OrStep<S> extends AbstractStep<S, S> implements TraversalHolder {

    private List<Traversal<S, ?>> orTraversals;

    public OrStep(final Traversal traversal, final Traversal<S, ?>... orTraversals) {
        super(traversal);
        this.orTraversals = Arrays.asList(orTraversals);
        for (final Traversal<S, ?> orTraversal : this.orTraversals) {
            this.executeTraversalOperations(orTraversal, TYPICAL_LOCAL_OPERATIONS);
        }
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        while (true) {
            final Traverser.Admin<S> start = this.starts.next();
            boolean found = false;
            for (final Traversal<S, ?> traversal : this.orTraversals) {
                traversal.asAdmin().addStart(start.split());
                found = traversal.hasNext();
                traversal.asAdmin().reset();
                if (found) break;
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
        return this.orTraversals;
    }

    @Override
    public OrStep<S> clone() throws CloneNotSupportedException {
        final OrStep<S> clone = (OrStep<S>) super.clone();
        clone.orTraversals = new ArrayList<>();
        for (final Traversal<S, ?> orTraversal : this.orTraversals) {
            final Traversal<S, ?> cloneOrTraversal = orTraversal.clone();
            clone.orTraversals.add(cloneOrTraversal);
            clone.executeTraversalOperations(cloneOrTraversal, TYPICAL_LOCAL_OPERATIONS);
        }
        return clone;
    }
}
