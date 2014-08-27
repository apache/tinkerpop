package com.tinkerpop.gremlin.process.computer.traversal.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.step.map.BackStep;
import com.tinkerpop.gremlin.process.graph.step.map.MapStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BackComputerStep<S, E> extends MapStep<S, E> implements PathConsumer {

    public String stepLabel;

    public BackComputerStep(final Traversal traversal, final BackStep backStep) {
        super(traversal);
        this.stepLabel = backStep.stepLabel;
        this.setFunction(traverser -> traverser.getPath().get(stepLabel));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.stepLabel);
    }
}
