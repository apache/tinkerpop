package com.tinkerpop.gremlin.process.graph.util;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultGraphTraversal<S, E> extends DefaultTraversal<S, E> implements GraphTraversal<S, E>, GraphTraversal.Admin<S, E> {

    public DefaultGraphTraversal(final Class emanatingClass) {
        super(emanatingClass);
    }

    @Override
    public GraphTraversal.Admin<S, E> asAdmin() {
        return this;
    }
}
