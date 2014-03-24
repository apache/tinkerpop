package com.tinkerpop.gremlin.util.function;

import java.io.Serializable;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface SFunction<A, B> extends Function<A, B>, Serializable {

    public static SFunction identity() {
        return a -> a;
    }
}
