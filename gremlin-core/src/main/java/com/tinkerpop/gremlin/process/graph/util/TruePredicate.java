package com.tinkerpop.gremlin.process.graph.util;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TruePredicate<S> implements Predicate<S> {

    private static final TruePredicate INSTANCE = new TruePredicate();

    private TruePredicate() {

    }

    @Override
    public boolean test(final S s) {
        return true;
    }

    @Override
    public String toString() {
        return "true";
    }

    public static <S> TruePredicate<S> instance() {
        return INSTANCE;
    }
}
