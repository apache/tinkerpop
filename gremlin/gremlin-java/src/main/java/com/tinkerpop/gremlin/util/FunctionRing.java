package com.tinkerpop.gremlin.util;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FunctionRing<A, B> {

    public Function<A, B>[] functions;
    public int currentFunction = 0;
    private static final Function IDENTITY = a -> a;

    public FunctionRing(final Function... functions) {
        this.functions = functions;
    }

    public Function<A, B> next() {
        if (this.functions.length == 0) {
            return IDENTITY;
        } else {
            final Function<A, B> nextFunction = this.functions[this.currentFunction];
            this.currentFunction = (this.currentFunction + 1) % this.functions.length;
            return nextFunction;
        }
    }

    public boolean hasFunctions() {
        return this.functions.length > 0;
    }
}
