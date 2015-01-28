package com.tinkerpop.gremlin.util.function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface CloneableLambda {

    public Object cloneLambda() throws CloneNotSupportedException;

    public static <C> C tryClone(final C lambda) throws CloneNotSupportedException {
        return lambda instanceof CloneableLambda ? (C) ((CloneableLambda) lambda).cloneLambda() : lambda;
    }

}
