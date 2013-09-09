package com.tinkerpop.gremlin;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Gremlin {

    public static int doLambda(Function<Integer, Integer> function) {
        return function.apply(1);
    }
}
