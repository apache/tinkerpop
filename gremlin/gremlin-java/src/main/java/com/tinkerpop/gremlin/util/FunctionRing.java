package com.tinkerpop.gremlin.util;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FunctionRing<A, B> {

    public Function<A, B>[] functions;
    private int currentFunction = -1;
    private static final Function IDENTITY = a -> a;

    public FunctionRing(final Function... functions) {
        this.functions = functions;
    }

    public Function<A, B> next() {
        if (this.functions.length == 0) {
            return IDENTITY;
        } else {
            this.currentFunction = (this.currentFunction + 1) % this.functions.length;
            return this.functions[this.currentFunction];
        }
    }

    public boolean hasFunctions() {
        return this.functions.length > 0;
    }
}
