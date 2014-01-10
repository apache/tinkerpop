package com.tinkerpop.blueprints;

import java.util.Collection;
import java.util.function.BiPredicate;

/**
 * {@link Contains} is a {@link BiPredicate} that evaluates whether the first object is contained within (or not
 * within) the second collection object. For example:
 * <p/>
 * <pre>
 * gremlin IN [gremlin, blueprints, furnace] == true
 * gremlin NOT_IN [gremlin, rexster] == false
 * rexster NOT_IN [gremlin, blueprints, furnace] == true
 * </pre>
 *
 * @author Pierre De Wilde
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Contains implements BiPredicate<Object, Object> {

    IN, NOT_IN;

    public boolean test(final Object first, final Object second) {
        if (second instanceof Collection)
            return this.equals(IN) ? ((Collection) second).contains(first) : !((Collection) second).contains(first);
        else if (second instanceof AnnotatedList) {
            final boolean exists = ((AnnotatedList) second).query().has(Annotations.Key.VALUE, first).limit(1).values().iterator().hasNext();
            return this.equals(IN) ? exists : !exists;
        } else
            throw new IllegalArgumentException("The provide argument must be either a Collection or AnnotatedList: " + second.getClass());
    }

    public Contains opposite() {
        return this.equals(IN) ? NOT_IN : IN;
    }
}
