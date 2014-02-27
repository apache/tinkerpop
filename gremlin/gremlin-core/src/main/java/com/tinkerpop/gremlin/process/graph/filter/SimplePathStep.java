package com.tinkerpop.gremlin.process.graph.filter;


import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.PathConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimplePathStep<S> extends FilterStep<S> implements PathConsumer {

    public SimplePathStep(final Traversal traversal) {
        super(traversal, holder -> holder.getPath().isSimple());
    }
}
