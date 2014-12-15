package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.util.FunctionRing;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface FunctionRingAcceptor<A, B> {

    public void setFunctionRing(final FunctionRing<A, B> functionRing);

    public static void singleFunctionSupported(final FunctionRing functionRing, final FunctionRingAcceptor step) {
        if (functionRing.size() > 1)
            throw new IllegalArgumentException(step.toString() + " does not support multiple functions");
    }
}
