package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.marker.GraphComputerAnalyzer;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BackStep<S, E> extends MapStep<S, E> implements PathConsumer, GraphComputerAnalyzer {

    public String stepLabel;
    private boolean requiresPaths = false;

    public BackStep(final Traversal traversal, final String stepLabel) {
        super(traversal);
        this.stepLabel = stepLabel;
        TraversalHelper.getStep(this.stepLabel, this.traversal);
        this.setBiFunction((traverser, sideEffects) -> this.requiresPaths() ? traverser.getPath().get(this.stepLabel) : sideEffects.get(this.stepLabel));
    }

    @Override
    public boolean requiresPaths() {
        return this.requiresPaths;
    }

    @Override
    public void registerGraphComputer(final GraphComputer computer) {
        this.requiresPaths = true;
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.stepLabel);
    }
}
