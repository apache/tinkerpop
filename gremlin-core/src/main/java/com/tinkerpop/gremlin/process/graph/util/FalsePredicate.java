package com.tinkerpop.gremlin.process.graph.util;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FalsePredicate<S> implements Predicate<S> {

    private static final FalsePredicate INSTANCE = new FalsePredicate();

    private FalsePredicate() {

    }

    @Override
    public boolean test(final S s) {
        return false;
    }

    @Override
    public String toString() {
        return "false";
    }

    public static <S> FalsePredicate<S> instance() {
        return INSTANCE;
    }
}