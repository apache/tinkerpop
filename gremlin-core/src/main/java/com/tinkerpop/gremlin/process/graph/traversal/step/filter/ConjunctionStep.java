package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import com.tinkerpop.gremlin.process.traversal.step.AbstractStep;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ConjunctionStep<S> extends AbstractStep<S, S> implements TraversalParent {

    private List<Traversal.Admin<S, ?>> conjunctionTraversals;
    private final boolean isAnd;

    public ConjunctionStep(final Traversal.Admin traversal, final Traversal.Admin<S, ?>... conjunctionTraversals) {
        super(traversal);
        this.isAnd = this.getClass().equals(AndStep.class);
        this.conjunctionTraversals = Arrays.asList(conjunctionTraversals);
        for (final Traversal.Admin<S, ?> conjunctionTraversal : this.conjunctionTraversals) {
            this.integrateChild(conjunctionTraversal, TYPICAL_LOCAL_OPERATIONS);
        }
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        while (true) {
            final Traverser.Admin<S> start = this.starts.next();
            boolean found = false;
            for (final Traversal.Admin<S, ?> traversal : this.conjunctionTraversals) {
                traversal.addStart(start.split());
                found = traversal.hasNext();
                traversal.reset();
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
        return this.getSelfAndChildRequirements();
    }

    @Override
    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        return this.conjunctionTraversals;
    }

    @Override
    public ConjunctionStep<S> clone() throws CloneNotSupportedException {
        final ConjunctionStep<S> clone = (ConjunctionStep<S>) super.clone();
        clone.conjunctionTraversals = new ArrayList<>();
        for (final Traversal.Admin<S, ?> andTraversal : this.conjunctionTraversals) {
            clone.conjunctionTraversals.add(clone.integrateChild(andTraversal.clone(), TYPICAL_LOCAL_OPERATIONS));
        }
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.conjunctionTraversals);
    }
}
