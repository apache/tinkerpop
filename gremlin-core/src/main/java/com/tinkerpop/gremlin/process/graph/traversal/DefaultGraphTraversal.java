package com.tinkerpop.gremlin.process.graph.traversal;

import com.tinkerpop.gremlin.process.traversal.DefaultTraversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultGraphTraversal<S, E> extends DefaultTraversal<S, E> implements GraphTraversal.Admin<S, E> {

    public DefaultGraphTraversal(final Class emanatingClass) {
        super(emanatingClass);
    }

    @Override
    public GraphTraversal.Admin<S, E> asAdmin() {
        return this;
    }

    @Override
    public GraphTraversal<S, E> iterate() {
        return GraphTraversal.Admin.super.iterate();
    }

    @Override
    public DefaultGraphTraversal<S, E> clone() throws CloneNotSupportedException {
        return (DefaultGraphTraversal<S, E>) super.clone();
    }
}
