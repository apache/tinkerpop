package com.tinkerpop.gremlin.process.graph.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.EmptyTraversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyGraphTraversal<S, E> extends EmptyTraversal<S, E> implements GraphTraversal<S, E> {

    private static final EmptyGraphTraversal INSTANCE = new EmptyGraphTraversal<>();

    public static <A,B> EmptyGraphTraversal<A,B> instance() {
        return INSTANCE;
    }

    private EmptyGraphTraversal() {

    }

    @Override
    public <E2> GraphTraversal<S, E2> addStep(final Step<?, E2> step) {
        return instance();
    }

    @Override
    public GraphTraversal<S, E> trackPaths() {
        return instance();
    }

    public GraphTraversal<S, E> submit(final GraphComputer computer) {
        return instance();
    }
}
