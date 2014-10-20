package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CyclicPathStep<S> extends FilterStep<S> implements PathConsumer, Reversible {

    public CyclicPathStep(final Traversal traversal) {
        super(traversal);
        this.setPredicate(traverser -> !traverser.path().isSimple());
    }
}
