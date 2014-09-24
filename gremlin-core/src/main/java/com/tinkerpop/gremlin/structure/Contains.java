package com.tinkerpop.gremlin.structure;

import java.util.Collection;

/**
 * {@link Contains} is a {@link java.util.function.BiPredicate} that evaluates whether the first object is contained within (or not
 * within) the second collection object. For example:
 * <p>
 * <pre>
 * gremlin IN [gremlin, blueprints, furnace] == true
 * gremlin NOT_IN [gremlin, rexster] == false
 * rexster NOT_IN [gremlin, blueprints, furnace] == true
 * </pre>
 *
 * @author Pierre De Wilde
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Contains implements java.util.function.BiPredicate<Object, Object>, java.io.Serializable {

    IN, NOT_IN;

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean test(final Object first, final Object second) {
        return this.equals(IN) ? ((Collection) second).contains(first) : !((Collection) second).contains(first);
    }

    /**
     * Produce the opposite representation of the current {@code Contains} object.
     */
    public Contains opposite() {
        return this.equals(IN) ? NOT_IN : IN;
    }
}
