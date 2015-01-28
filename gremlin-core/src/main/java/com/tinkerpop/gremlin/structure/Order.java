package com.tinkerpop.gremlin.structure;

import java.util.Comparator;
import java.util.Map;
import java.util.Random;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Order implements Comparator {

    incr {
        @Override
        public int compare(final Object first, final Object second) {
            return Comparator.<Comparable>naturalOrder().compare((Comparable) first, (Comparable) second);
        }
    }, decr {
        @Override
        public int compare(final Object first, final Object second) {
            return Comparator.<Comparable>reverseOrder().compare((Comparable) first, (Comparable) second);
        }
    }, keyIncr {
        @Override
        public int compare(final Object first, final Object second) {
            return Comparator.<Comparable>naturalOrder().compare(((Map.Entry<Comparable, ?>) first).getKey(), ((Map.Entry<Comparable, ?>) second).getKey());
        }
    }, valueIncr {
        @Override
        public int compare(final Object first, final Object second) {
            return Comparator.<Comparable>naturalOrder().compare(((Map.Entry<?, Comparable>) first).getValue(), ((Map.Entry<?, Comparable>) second).getValue());
        }
    }, keyDecr {
        @Override
        public int compare(final Object first, final Object second) {
            return Comparator.<Comparable>reverseOrder().compare(((Map.Entry<Comparable, ?>) first).getKey(), ((Map.Entry<Comparable, ?>) second).getKey());
        }
    }, valueDecr {
        @Override
        public int compare(final Object first, final Object second) {
            return Comparator.<Comparable>reverseOrder().compare(((Map.Entry<?, Comparable>) first).getValue(), ((Map.Entry<?, Comparable>) second).getValue());
        }
    }, shuffle {
        @Override
        public int compare(final Object first, final Object second) {
            return RANDOM.nextBoolean() ? -1 : 1;
        }
    };

    private static final Random RANDOM = new Random();

    /**
     * {@inheritDoc}
     */
    public abstract int compare(final Object first, final Object second);

    /**
     * Produce the opposite representation of the current {@code Order} enum.
     */
    public Order opposite() {
        return this.equals(incr) ? decr : incr;  // TODO
    }
}