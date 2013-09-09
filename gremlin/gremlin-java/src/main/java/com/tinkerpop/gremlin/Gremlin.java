package com.tinkerpop.gremlin;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Gremlin {

    public static void doLambda(Function<Integer, Integer> function) {
        int x = function.apply(1);
        doLambda(y -> x + y);
    }
}
