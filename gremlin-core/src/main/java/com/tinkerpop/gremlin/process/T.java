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
    nin,
    /**
     * Label (representing Element.label())
     */
    label,
    /**
     * Id (representing Element.id())
     */
    id,
    /**
     * Key (representing Property.key())
     */
    key,
    /**
     * Value (representing Property.value())
     */
    value;


    public SBiPredicate getPredicate() {
        if (this.equals(T.eq))
            return Compare.EQUAL;
        else if (this.equals(T.neq))
            return Compare.NOT_EQUAL;
        else if (this.equals(T.lt))
            return Compare.LESS_THAN;
        else if (this.equals(T.lte))
            return Compare.LESS_THAN_EQUAL;
        else if (this.equals(T.gt))
            return Compare.GREATER_THAN;
        else if (this.equals(T.gte))
            return Compare.GREATER_THAN_EQUAL;
        else if (this.equals(T.in))
            return Contains.IN;
        else if (this.equals(T.nin))
            return Contains.NOT_IN;
        else
            throw new IllegalArgumentException(this.toString() + " is an unknown predicate type");
    }

    public Comparator getComparator() {
        if (this.equals(T.decr))
            return Comparator.reverseOrder();
        else if (this.equals(T.incr))
            return Comparator.naturalOrder();
        else
            throw new IllegalArgumentException(this.toString() + " is an unknown comparator type");
    }

    public static final String LABEL = "%&%label";
    public static final String ID = "%&%id";
    public static final String KEY = "%&%key";
    public static final String VALUE = "%&%value";

    public String getAccessor() {
        if (this.equals(T.label))
            return LABEL;
        else if (this.equals(T.id))
            return ID;
        else if (this.equals(T.key))
            return KEY;
        else if (this.equals(T.value))
            return VALUE;
        else
            throw new IllegalArgumentException(this.toString() + " is an unknown accessor type");
    }

}
