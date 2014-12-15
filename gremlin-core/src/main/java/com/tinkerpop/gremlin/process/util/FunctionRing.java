package com.tinkerpop.gremlin.process.util;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FunctionRing<A, B> implements Cloneable {

    public Function<A, B>[] functions;
    private int currentFunction = -1;

    public FunctionRing(final Function... functions) {
        this.functions = functions;
    }

    public Function<A, B> next() {
        if (this.functions.length == 0) {
            return (Function<A, B>) Function.identity();
        } else {
            this.currentFunction = (this.currentFunction + 1) % this.functions.length;
            return this.functions[this.currentFunction];
        }
    }

    public boolean hasFunctions() {
        return this.functions.length > 0;
    }

    public void reset() {
        this.currentFunction = -1;
    }

    public int size() {
        return this.functions.length;
    }

    public boolean roundComplete() {
        return this.currentFunction == this.functions.length - 1;
    }

    public FunctionRing<A, B> clone() throws CloneNotSupportedException {
        return new FunctionRing<>(this.functions);
    }
}
