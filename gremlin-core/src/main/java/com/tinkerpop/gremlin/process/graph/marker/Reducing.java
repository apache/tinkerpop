package com.tinkerpop.gremlin.process.graph.marker;

import org.javatuples.Pair;

import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Reducing<A, B> {

    public Pair<Supplier<A>, BiFunction<A, B, A>> getReducer();
}
