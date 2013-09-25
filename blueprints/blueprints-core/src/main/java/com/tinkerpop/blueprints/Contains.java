package com.tinkerpop.blueprints;

import java.util.Collection;
import java.util.function.BiPredicate;

/**
 * Contains is a predicate that evaluates whether the first object is contained within (or not within) the second collection object.
 * For example:
 * gremlin IN [gremlin, blueprints, furnace] == true
 * gremlin NOT_IN [gremlin, rexster] == false
 * rexster NOT_IN [gremlin, blueprints, furnace] == true
 *
 * @author Pierre De Wilde
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Contains implements BiPredicate<Object, Collection> {

    IN, NOT_IN;

    public boolean test(final Object first, final Collection second) {
        return this.equals(IN) ? second.contains(first) : !second.contains(first);
    }

    public Contains opposite() {
        return this.equals(IN) ? NOT_IN : IN;
    }
}
