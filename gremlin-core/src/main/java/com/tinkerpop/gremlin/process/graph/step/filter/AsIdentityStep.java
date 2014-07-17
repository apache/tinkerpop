package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AsIdentityStep<S> extends FilterStep<S> implements PathConsumer {

    public AsIdentityStep(final Traversal traversal) {
        super(traversal);
        this.setPredicate(traverser -> true);
    }
}
