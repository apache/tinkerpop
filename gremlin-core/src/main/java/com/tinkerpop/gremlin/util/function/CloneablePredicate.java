package com.tinkerpop.gremlin.util.function;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface CloneablePredicate<A> extends Predicate<A>, Cloneable {

    public CloneablePredicate<A> clone() throws CloneNotSupportedException;

    public static <A> CloneablePredicate<A> clone(final CloneablePredicate<A> cloneablePredicate) {
        try {
            return cloneablePredicate.clone();
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
