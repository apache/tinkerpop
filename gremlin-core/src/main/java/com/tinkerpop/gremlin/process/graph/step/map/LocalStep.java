package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LocalStep<S, E> extends FlatMapStep<S, E> implements PathConsumer, TraversalHolder<S, E> {

    private Traversal<S, E> localTraversal;

    public LocalStep(final Traversal traversal, final Traversal<S, E> localTraversal) {
        super(traversal);
        this.localTraversal = localTraversal;
        this.localTraversal.asAdmin().setStrategies(this.getTraversal().asAdmin().getStrategies());
        LocalStep.generateFunction(this);
    }

    @Override
    public LocalStep<S, E> clone() throws CloneNotSupportedException {
        final LocalStep<S, E> clone = (LocalStep<S, E>) super.clone();
        clone.localTraversal = this.localTraversal.clone();
        LocalStep.generateFunction(clone);
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.localTraversal);
    }

    @Override
    public Collection<Traversal<S, E>> getTraversals() {
        return Collections.singletonList(this.localTraversal);
    }

    @Override
    public boolean requiresPaths() {
        return TraversalHelper.trackPaths(this.localTraversal);
    }

    ////////////////

    private static final <S, E> void generateFunction(final LocalStep<S, E> localStep) {
        localStep.setFunction(traverser -> {
            // TODO: traverser.asAdmin().mergeSideEffects(localStep.localTraversal.sideEffects());
            localStep.localTraversal.asAdmin().reset();
            TraversalHelper.getStart(localStep.localTraversal).addStart(traverser);
            return localStep.localTraversal;
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
