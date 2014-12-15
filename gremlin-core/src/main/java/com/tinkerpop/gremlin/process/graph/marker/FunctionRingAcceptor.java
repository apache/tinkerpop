package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.util.FunctionRing;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface FunctionRingAcceptor<A, B> {

    public void setFunctionRing(final FunctionRing<A, B> functionRing);
}
