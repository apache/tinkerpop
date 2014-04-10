package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.PathConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CyclicPathStep<S> extends FilterStep<S> implements PathConsumer {

    public CyclicPathStep(final Traversal traversal) {
        super(traversal);
        this.setPredicate(holder -> !holder.getPath().isSimple());
    }
}
