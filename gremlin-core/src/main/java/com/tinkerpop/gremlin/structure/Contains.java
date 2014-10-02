package com.tinkerpop.gremlin.structure;

import java.util.Collection;
import java.util.function.BiPredicate;

/**
 * {@link Contains} is a {@link java.util.function.BiPredicate} that evaluates whether the first object is contained within (or not
 * within) the second collection object. For example:
 * <p/>
 * <pre>
 * gremlin Contains.in [gremlin, blueprints, furnace] == true
 * gremlin Contains.nin [gremlin, rexster] == false
 * rexster Contains.nin [gremlin, blueprints, furnace] == true
 * </pre>
 *
 * @author Pierre De Wilde
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Contains implements BiPredicate<Object, Collection> {

    in {
        @Override
        public boolean test(final Object first, final Collection second) {
            return second.contains(first);
        }
    }, nin {
        @Override
        public boolean test(final Object first, final Collection second) {
            return !second.contains(first);
        }
    };

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract boolean test(final Object first, final Collection second);

    /**
     * Produce the opposite representation of the current {@code Contains} enum.
     */
    public Contains opposite() {
        return this.equals(in) ? nin : in;
    }
}
