package com.tinkerpop.gremlin;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Optimizer {

    public enum Rate {
        RUNTIME,
        COMPILE_TIME
    }

    public <S, E> Pipeline<S, E> optimize(final Pipeline<S, E> pipeline);

    public Rate getOptimizationRate();
}
