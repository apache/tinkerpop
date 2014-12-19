package com.tinkerpop.gremlin.process.graph.marker;

import java.util.List;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface FunctionHolder<A, B> {

    public void addFunction(final Function<A, B> function);

    public List<Function<A, B>> getFunctions();

}
