package com.tinkerpop.gremlin.structure;

import java.util.List;
import java.util.function.BiPredicate;

/**
 * {@link Compare} is a {@link java.util.function.BiPredicate} that determines whether the first argument is {@code ==}, {@code !=},
 * {@code >}, {@code >=}, {@code <}, {@code <=} to the second argument.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Compare implements BiPredicate<Object, Object> {

    eq {
        @Override
        public boolean test(final Object first, final Object second) {
            if (null == first)
                return second == null;
            return first.equals(second);
        }

        @Override
        public Compare opposite() {
            return neq;
        }
    }, neq {
        @Override
        public boolean test(final Object first, final Object second) {
            if (null == first)
                return second != null;
            return !first.equals(second);
        }

        @Override
        public Compare opposite() {
            return eq;
        }
    }, gt {
        @Override
        public boolean test(final Object first, final Object second) {
            return !(null == first || second == null) && ((Comparable) first).compareTo(second) >= 1;
        }

        @Override
        public Compare opposite() {
            return lte;
        }
    }, gte {
        @Override
        public boolean test(final Object first, final Object second) {
            return !(null == first || second == null) && ((Comparable) first).compareTo(second) >= 0;
        }

        @Override
        public Compare opposite() {
            return lt;
        }
    }, lt {
        @Override
        public boolean test(final Object first, final Object second) {
            return !(null == first || second == null) && ((Comparable) first).compareTo(second) <= -1;
        }

        @Override
        public Compare opposite() {
            return gte;
        }
    }, lte {
        @Override
        public boolean test(final Object first, final Object second) {
            return !(null == first || second == null) && ((Comparable) first).compareTo(second) <= 0;
        }

        @Override
        public Compare opposite() {
            return gt;
        }
    }, inside {
        @Override
        public boolean test(final Object first, final Object second) {
            return !(null == first || second == null) && gt.test(first, ((List) second).get(0)) && lt.test(first, ((List) second).get(1));
        }

        @Override
        public Compare opposite() {
            return outside;
        }
    }, outside {
        @Override
        public boolean test(final Object first, final Object second) {
            return !(null == first || second == null) && lt.test(first, ((List) second).get(0)) || gt.test(first, ((List) second).get(1));
        }

        @Override
        public Compare opposite() {
            return inside;
        }
    };

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract boolean test(final Object first, final Object second);

    /**
     * Produce the opposite representation of the current {@code Compare} enum.
     */
    public abstract Compare opposite();
}
