package com.tinkerpop.gremlin.process.graph.marker;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface FunctionAcceptor<A, B> {

    public void addFunction(final Function<A, B> function);

}
