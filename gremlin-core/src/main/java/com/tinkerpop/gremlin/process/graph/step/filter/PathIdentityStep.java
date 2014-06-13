package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathIdentityStep<S> extends FilterStep<S> implements PathConsumer {

    public PathIdentityStep(final Traversal traversal) {
        super(traversal);
        this.setPredicate(traverser -> true);
    }
}
