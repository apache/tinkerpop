package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BackStep<S, E> extends MapStep<S, E> implements PathConsumer, EngineDependent {

    private final String stepLabel;
    private boolean requiresPaths = false;

    public BackStep(final Traversal traversal, final String stepLabel) {
        super(traversal);
        this.stepLabel = stepLabel;
        TraversalHelper.getStep(this.stepLabel, this.traversal);
        this.setFunction(traverser -> this.requiresPaths() ? traverser.path().get(this.stepLabel) : traverser.get(this.stepLabel));
    }

    @Override
    public boolean requiresPaths() {
        return this.requiresPaths;
    }

    @Override
    public void onEngine(final Engine engine) {
        this.requiresPaths = engine.equals(Engine.COMPUTER);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.stepLabel);
    }
}
