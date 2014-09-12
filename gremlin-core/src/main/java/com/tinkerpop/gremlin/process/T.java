package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.util.function.SBiPredicate;

import java.util.Comparator;

/**
 * A collection of (T)okens which allows for more concise Traversal definitions.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum T {
    /**
     * Greater than
     */
    gt,
    /**
     * Less than
     */
    lt,
    /**
     * Equal to
     */
    eq,
    /**
     * Greater than or equal to
     */
    gte,
    /**
     * Less than or equal to
     */
    lte,
    /**
     * Not equal to
     */
    neq,
    /**
     * Decrement
     */
    decr,
    /**
     * Increment
     */
    incr,
    /**
     * In collection
     */
    in,
    /**
     * Not in collection
     */
    nin;


    public static SBiPredicate convertPredicate(final T t) {
        if (t.equals(T.eq))
            return Compare.EQUAL;
        else if (t.equals(T.neq))
            return Compare.NOT_EQUAL;
        else if (t.equals(T.lt))
            return Compare.LESS_THAN;
        else if (t.equals(T.lte))
            return Compare.LESS_THAN_EQUAL;
        else if (t.equals(T.gt))
            return Compare.GREATER_THAN;
        else if (t.equals(T.gte))
            return Compare.GREATER_THAN_EQUAL;
        else if (t.equals(T.in))
            return Contains.IN;
        else if (t.equals(T.nin))
            return Contains.NOT_IN;
        else
            throw new IllegalArgumentException(t.toString() + " is an unknown predicate type");
    }

    public static Comparator convertComparator(final T t) {
        if (t.equals(T.decr))
            return Comparator.reverseOrder();
        else if (t.equals(T.incr))
            return Comparator.naturalOrder();
        else
            throw new IllegalArgumentException(t.toString() + " is an unknown comparator type");
    }


}
