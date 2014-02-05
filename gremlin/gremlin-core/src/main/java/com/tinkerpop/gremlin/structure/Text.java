package com.tinkerpop.gremlin.structure;

import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Text implements BiPredicate<String, String> {

    CONTAINS, REGEX, PREFIX;

    public boolean test(final String first, final String second) {
        if (this.equals(CONTAINS)) {
            return first.contains(second);
        } else if (this.equals(REGEX)) {
            return second.matches(first);
        } else {
            return first.startsWith(second);
        }
    }
}
