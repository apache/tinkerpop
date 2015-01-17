package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LocalStep<S, E> extends FlatMapStep<S, E> implements TraversalHolder<S, E>, EngineDependent {

    private static final Child[] CHILD_OPERATIONS = new Child[]{Child.SET_HOLDER, Child.SET_STRATEGIES}; // TODO: Nest.SET/MERGE_SIDE_EFFECTS?

    private Traversal<S, E> localTraversal;
    private boolean onGraphComputer = false;

    public LocalStep(final Traversal traversal, final Traversal<S, E> localTraversal) {
        super(traversal);
        this.localTraversal = localTraversal;
        this.executeTraversalOperations(CHILD_OPERATIONS);
        LocalStep.generateFunction(this);
    }

    @Override
    public LocalStep<S, E> clone() throws CloneNotSupportedException {
        final LocalStep<S, E> clone = (LocalStep<S, E>) super.clone();
        clone.localTraversal = this.localTraversal.clone();
        clone.executeTraversalOperations(CHILD_OPERATIONS);
        LocalStep.generateFunction(clone);
        return clone;
    }

    @Override
    public void onEngine(final TraversalEngine engine) {
        this.onGraphComputer = engine.equals(TraversalEngine.COMPUTER);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.localTraversal);
    }

    @Override
    public List<Traversal<S, E>> getTraversals() {
        return Collections.singletonList(this.localTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getTraversalRequirements();
    }

    @Override
    public void reset() {
        super.reset();
        this.resetTraversals();
    }

    ////////////////

    private static final <S, E> void generateFunction(final LocalStep<S, E> localStep) {
        localStep.setFunction(traverser -> {
            localStep.localTraversal.asAdmin().reset();
            localStep.localTraversal.asAdmin().addStart(traverser);
            return localStep.onGraphComputer ? localStep.localTraversal.toList().iterator() : localStep.localTraversal;
            // TODO: This sucks. Why do I need to create a list on GraphComputer?
        });
    }

    public boolean isLocalStarGraph() {
        final List<Step<?, ?>> steps = this.traversal.asAdmin().getSteps();
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
