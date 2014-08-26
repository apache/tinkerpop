package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BackStep<S, E> extends MapStep<S, E> implements PathConsumer {

    public String stepLabel;

    public BackStep(final Traversal traversal, final String stepLabel) {
        super(traversal);
        this.stepLabel = stepLabel;
        this.setFunction(traverser -> traverser.getPath().get(this.stepLabel));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.stepLabel);
    }
}
