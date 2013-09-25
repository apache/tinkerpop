package com.tinkerpop.blueprints;

import java.util.function.BiPredicate;

/**
 * Compare is a predicate that determines whether the first argument is ==, !=, &gt;, &gt;=, &lt;, &lt;= to the second argument.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Compare implements BiPredicate<Object, Object> {

    EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL;

    public boolean test(final Object first, final Object second) {
        switch (this) {
            case EQUAL:
                if (null == first)
                    return second == null;
                return first.equals(second);
            case NOT_EQUAL:
                if (null == first)
                    return second != null;
                return !first.equals(second);
            case GREATER_THAN:
                if (null == first || second == null)
                    return false;
                return ((Comparable) first).compareTo(second) >= 1;
            case LESS_THAN:
                if (null == first || second == null)
                    return false;
                return ((Comparable) first).compareTo(second) <= -1;
            case GREATER_THAN_EQUAL:
                if (null == first || second == null)
                    return false;
                return ((Comparable) first).compareTo(second) >= 0;
            case LESS_THAN_EQUAL:
                if (null == first || second == null)
                    return false;
                return ((Comparable) first).compareTo(second) <= 0;
            default:
                throw new IllegalStateException("Invalid state as no valid compare was provided");
        }
    }

    public Compare opposite() {
        if (this.equals(EQUAL))
            return NOT_EQUAL;
        else if (this.equals(NOT_EQUAL))
            return EQUAL;
        else if (this.equals(GREATER_THAN))
            return LESS_THAN_EQUAL;
        else if (this.equals(GREATER_THAN_EQUAL))
            return LESS_THAN;
        else if (this.equals(LESS_THAN))
            return GREATER_THAN_EQUAL;
        else if (this.equals(LESS_THAN_EQUAL))
            return GREATER_THAN;
        else
            throw new IllegalStateException("Comparator does not have an opposite");
    }


    public String asString() {
        if (this.equals(EQUAL))
            return "=";
        else if (this.equals(GREATER_THAN))
            return ">";
        else if (this.equals(GREATER_THAN_EQUAL))
            return ">=";
        else if (this.equals(LESS_THAN_EQUAL))
            return "<=";
        else if (this.equals(LESS_THAN))
            return "<";
        else if (this.equals(NOT_EQUAL))
            return "<>";
        else
            throw new IllegalStateException("Comparator does not have a string representation");
    }


    public static Compare fromString(final String c) {
        if (c.equals("="))
            return EQUAL;
        else if (c.equals("<>"))
            return NOT_EQUAL;
        else if (c.equals(">"))
            return GREATER_THAN;
        else if (c.equals(">="))
            return GREATER_THAN_EQUAL;
        else if (c.equals("<"))
            return LESS_THAN;
        else if (c.equals("<="))
            return LESS_THAN_EQUAL;
        else
            throw new IllegalArgumentException("String representation does not match any comparator");
    }
}
