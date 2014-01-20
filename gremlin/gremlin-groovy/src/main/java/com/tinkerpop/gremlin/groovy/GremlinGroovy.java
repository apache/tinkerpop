package com.tinkerpop.gremlin.groovy;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.gremlin.EmptyGraph;
import com.tinkerpop.gremlin.Gremlin;
import groovy.lang.Closure;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinGroovy<S, E> extends Gremlin<S, E> {

    protected GremlinGroovy(final Graph graph, final boolean useDefaultOptimizers) {
        super(graph, useDefaultOptimizers);
    }

    public static GremlinGroovy<?, ?> of() {
        return new GremlinGroovy<>(EmptyGraph.instance(),true);
    }

    public static GremlinGroovy<?, ?> of(final Graph graph) {
        return new GremlinGroovy(graph, true);
    }

    public static GremlinGroovy<?, ?> of(final Graph graph, final boolean useDefaultOptimizers) {
        return new GremlinGroovy(graph, useDefaultOptimizers);
    }

    public GremlinGroovy<S, E> jump(final String as, final Closure ifClosure, final Closure emitClosure) {
        return (GremlinGroovy<S, E>) super.jump(as, new GroovyPredicate<>(ifClosure), new GroovyPredicate<>(emitClosure));
    }

    public GremlinGroovy<S, E> jump(final String as, final Closure ifClosure) {
        return (GremlinGroovy<S, E>) super.jump(as, new GroovyPredicate<>(ifClosure));
    }

    public GremlinGroovy<S, E> jump(final String as) {
        return (GremlinGroovy<S, E>) super.jump(as);
    }
}
