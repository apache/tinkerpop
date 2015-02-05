package com.tinkerpop.gremlin.process.graph.traversal.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import com.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class CoalesceStep<S, E> extends FlatMapStep<S, E> implements TraversalParent {

    private List<Traversal.Admin<S, E>> coalesceTraversals;

    public CoalesceStep(final Traversal.Admin traversal, final Traversal.Admin<S, E>... coalesceTraversals) {
        super(traversal);
        this.coalesceTraversals = Arrays.asList(coalesceTraversals);
        for (final Traversal.Admin<S, ?> conjunctionTraversal : this.coalesceTraversals) {
            this.integrateChild(conjunctionTraversal, TYPICAL_LOCAL_OPERATIONS);
        }
        this.setFunction(traverser -> {
            for (final Traversal.Admin<S, E> coalesceTraversal : this.coalesceTraversals) {
                coalesceTraversal.reset();
                coalesceTraversal.addStart(traverser);
                if (coalesceTraversal.hasNext())
                    return coalesceTraversal;
            }
            return EmptyTraversal.instance();
        });
        CoalesceStep.generateFunction(this);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return this.coalesceTraversals;
    }

    @Override
    public CoalesceStep<S, E> clone() throws CloneNotSupportedException {
        final CoalesceStep<S, E> clone = (CoalesceStep<S, E>) super.clone();
        clone.coalesceTraversals = new ArrayList<>();
        for (final Traversal.Admin<S, ?> conjunctionTraversal : this.coalesceTraversals) {
            clone.coalesceTraversals.add(clone.integrateChild(conjunctionTraversal.clone(), TYPICAL_LOCAL_OPERATIONS));
        }
        CoalesceStep.generateFunction(clone);
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.coalesceTraversals);
    }

    private static <S, E> void generateFunction(final CoalesceStep<S, E> coalesceStep) {
        coalesceStep.setFunction(traverser -> {
            for (final Traversal.Admin<S, E> coalesceTraversal : coalesceStep.coalesceTraversals) {
                coalesceTraversal.reset();
                coalesceTraversal.addStart(traverser);
                if (coalesceTraversal.hasNext())
                    return coalesceTraversal;
            }
            return EmptyTraversal.instance();
        });
    }
}
