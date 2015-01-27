package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.util.function.CloneableLambda;
import com.tinkerpop.gremlin.util.function.ResettableLambda;
import com.tinkerpop.gremlin.util.function.TraversableLambda;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FunctionRing<A, B> implements Cloneable {

    private List<Function<A, B>> functions = new ArrayList<>();
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
        this.functions.forEach(ResettableLambda::resetOrReturn);
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

    public List<Traversal<A, B>> getTraversals() {
        return this.functions.stream()
                .filter(function -> function instanceof TraversableLambda)
                .map(function -> ((TraversableLambda<A,B>) function).getTraversal())
                .collect(Collectors.toList());
    }

    public FunctionRing<A, B> clone() throws CloneNotSupportedException {
        final FunctionRing<A, B> clone = (FunctionRing<A, B>) super.clone();
        clone.functions = new ArrayList<>();
        clone.currentFunction = -1;
        for (final Function<A, B> function : this.functions) {
            clone.functions.add(CloneableLambda.cloneOrReturn(function));
        }
        return clone;
    }

    @Override
    public String toString() {
        return this.functions.toString();
    }
}
