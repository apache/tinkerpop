package com.tinkerpop.gremlin;

import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Gremlin {

    public static String doLambda(BiFunction<Integer, String, String> function) {
        return function.apply(1, "2");
    }

    public static String doTri(TriFunction<Integer,String,String,String> function) {
        return function.blah(1,"2","3");
    }
}
