package com.tinkerpop.gremlin.structure;

import java.util.Comparator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Order implements Comparator<Comparable> {

    incr {
        public int compare(final Comparable first, final Comparable second) {
            return Comparator.<Comparable>naturalOrder().compare(first, second);
        }
    }, decr {
        public int compare(final Comparable first, final Comparable second) {
            return Comparator.<Comparable>reverseOrder().compare(first, second);
        }
    };

    /**
     * {@inheritDoc}
     */
    public abstract int compare(final Comparable first, final Comparable second);

    /**
     * Produce the opposite representation of the current {@code Order} enum.
     */
    public Order opposite() {
        return this.equals(incr) ? decr : incr;
    }
}