package com.tinkerpop.gremlin.structure;

import java.util.function.BiPredicate;

/**
 * {@link Compare} is a {@link java.util.function.BiPredicate} that determines whether the first argument is {@code ==}, {@code !=},
 * {@code >}, {@code >=}, {@code <}, {@code <=} to the second argument.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Compare implements BiPredicate<Object, Object> {

    eq {
        public boolean test(final Object first, final Object second) {
            if (null == first)
                return second == null;
            return first.equals(second);
        }
    }, neq {
        public boolean test(final Object first, final Object second) {
            if (null == first)
                return second != null;
            return !first.equals(second);
        }
    }, gt {
        public boolean test(final Object first, final Object second) {
            return !(null == first || second == null) && ((Comparable) first).compareTo(second) >= 1;
        }

    }, gte {
        public boolean test(final Object first, final Object second) {
            return !(null == first || second == null) && ((Comparable) first).compareTo(second) >= 0;
        }
    }, lt {
        public boolean test(final Object first, final Object second) {
            return !(null == first || second == null) && ((Comparable) first).compareTo(second) <= -1;
        }
    }, lte {
        public boolean test(final Object first, final Object second) {
            return !(null == first || second == null) && ((Comparable) first).compareTo(second) <= 0;
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
    public Compare opposite() {
        if (this.equals(eq))
            return neq;
        else if (this.equals(neq))
            return eq;
        else if (this.equals(gt))
            return lte;
        else if (this.equals(gte))
            return lt;
        else if (this.equals(lt))
            return gte;
        else if (this.equals(lte))
            return gt;
        else
            throw new IllegalStateException("Comparator does not have an opposite");
    }

    /**
     * Gets the operator representation of the {@code Compare} object.
     */
    public String asString() {
        if (this.equals(eq))
            return "=";
        else if (this.equals(gt))
            return ">";
        else if (this.equals(gte))
            return ">=";
        else if (this.equals(lte))
            return "<=";
        else if (this.equals(lt))
            return "<";
        else if (this.equals(neq))
            return "<>";
        else
            throw new IllegalStateException("Comparator does not have a string representation");
    }

    /**
     * Get the {@code Compare} value based on the operator that represents it.
     */
    public static Compare fromString(final String c) {
        if (c.equals("="))
            return eq;
        else if (c.equals("<>"))
            return neq;
        else if (c.equals(">"))
            return gt;
        else if (c.equals(">="))
            return gte;
        else if (c.equals("<"))
            return lt;
        else if (c.equals("<="))
            return lte;
        else
            throw new IllegalArgumentException("String representation does not match any comparator");
    }
}
