package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SelectOneStep<S, E> extends SelectStep {

    private final Function<Traverser<S>, Map<String, E>> tempFunction;
    private final String selectLabel;

    public SelectOneStep(final Traversal traversal, final String selectLabel) {
        super(traversal, selectLabel);
        this.selectLabel = selectLabel;
        this.tempFunction = this.selectFunction;
        this.setFunction(traverser -> this.tempFunction.apply(((Traverser<S>) traverser)).get(this.selectLabel));
    }

    @Override
    public SelectOneStep<S, E> clone() throws CloneNotSupportedException {
        final SelectOneStep<S, E> clone = (SelectOneStep<S, E>) super.clone();
        clone.setFunction(traverser -> this.tempFunction.apply(((Traverser<S>) traverser)).get(this.selectLabel));
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.selectLabel, this.functionRing);
    }
}


