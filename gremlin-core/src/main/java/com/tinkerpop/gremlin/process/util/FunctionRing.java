package com.tinkerpop.gremlin.process.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FunctionRing<A, B> implements Cloneable {

    private final List<Function<A, B>> functions = new ArrayList<>();
    private int currentFunction = -1;

    public FunctionRing(final Function... functions) {
        for (final Function function : functions) {
            this.functions.add(function);
        }
    }

    public Function<A, B> next() {
        if (this.functions.size() == 0) {
            return (Function<A, B>) Function.identity();
        } else {
            this.currentFunction = (this.currentFunction + 1) % this.functions.size();
            return this.functions.get(this.currentFunction);
        }
    }

    public boolean isEmpty() {
        return this.functions.isEmpty();
    }

    public void reset() {
        this.currentFunction = -1;
    }

    public int size() {
        return this.functions.size();
    }

    public void addFunction(final Function<A, B> function) {
        this.functions.add(function);
    }

    public List<Function<A, B>> getFunctions() {
        return this.functions;
    }

    public FunctionRing<A, B> clone() throws CloneNotSupportedException {
        return new FunctionRing<>(this.functions.toArray(new Function[this.functions.size()]));
    }

    @Override
    public String toString() {
        return this.functions.toString();
    }
}
