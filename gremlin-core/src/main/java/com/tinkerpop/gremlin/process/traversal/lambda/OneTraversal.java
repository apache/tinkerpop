package com.tinkerpop.gremlin.process.traversal.lambda;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OneTraversal<S> extends AbstractLambdaTraversal<S, Number> {

    private static final OneTraversal INSTANCE = new OneTraversal<>();

    @Override
    public Number next() {
        return 1.0d;
    }

    @Override
    public String toString() {
        return "1.0";
    }

    @Override
    public OneTraversal<S> clone() throws CloneNotSupportedException {
        return INSTANCE;
    }

    public static <A> OneTraversal<A> instance() {
        return INSTANCE;
    }


}

