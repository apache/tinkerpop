package com.tinkerpop.gremlin.util.function;

import java.io.Serializable;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface SBiFunction<A, B, C> extends BiFunction<A, B, C>, Serializable {
}
