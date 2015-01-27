package com.tinkerpop.gremlin.util.function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ResettableLambda {

    public void reset();

    public static void resetOrReturn(final Object lambda) {
        if (lambda instanceof ResettableLambda)
            ((ResettableLambda) lambda).reset();
    }

}
