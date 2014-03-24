package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.util.function.SFunction;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FunctionRing<A, B> implements Serializable {

    public SFunction<A, B>[] functions;
    private int currentFunction = -1;

    public FunctionRing(final SFunction... functions) {
        this.functions = functions;
    }

    public SFunction<A, B> next() {
        if (this.functions.length == 0) {
            return SFunction.identity();
        } else {
            this.currentFunction = (this.currentFunction + 1) % this.functions.length;
            return this.functions[this.currentFunction];
        }
    }

    public boolean hasFunctions() {
        return this.functions.length > 0;
    }
}
