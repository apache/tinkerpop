package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LocalStep<S, E> extends FlatMapStep<S, E> implements TraversalHolder {

    private Traversal.Admin<S, E> localTraversal;

    public LocalStep(final Traversal traversal, final Traversal<S, E> localTraversal) {
        super(traversal);
        this.localTraversal = localTraversal.asAdmin();
        this.executeTraversalOperations(this.localTraversal, Child.SET_HOLDER);
        LocalStep.generateFunction(this);
    }

    @Override
    public LocalStep<S, E> clone() throws CloneNotSupportedException {
        final LocalStep<S, E> clone = (LocalStep<S, E>) super.clone();
        clone.localTraversal = this.localTraversal.clone().asAdmin();
        clone.executeTraversalOperations(clone.localTraversal, Child.SET_HOLDER);
        LocalStep.generateFunction(clone);
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.localTraversal);
    }

    @Override
    public List<Traversal<S, E>> getLocalTraversals() {
        return Collections.singletonList(this.localTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return TraversalHelper.getRequirements(this.localTraversal);
    }

    @Override
    public void reset() {
        super.reset();
        this.resetTraversals();
    }

    ////////////////

    private static final <S, E> void generateFunction(final LocalStep<S, E> localStep) {
        localStep.setFunction(traverser -> {
            final Traverser<S> localSplit = traverser.asAdmin().split();
            localStep.localTraversal.asAdmin().reset();    // is this needed?
            localSplit.asAdmin().setSideEffects(localStep.localTraversal.asAdmin().getSideEffects()); // set to the local traversals sideEffects
            localStep.localTraversal.asAdmin().addStart(localSplit);
            return localStep.localTraversal;
        });
    }

    public boolean isLocalStarGraph() {
        final List<Step> steps = this.localTraversal.asAdmin().getSteps();
        boolean foundOneVertexStep = false;
        for (final Step step : steps) {
            if (step instanceof VertexStep) {
                if (foundOneVertexStep) {
                    return false;
                } else {
                    foundOneVertexStep = true;
                }
            }
        }
        return true;
    }
}
