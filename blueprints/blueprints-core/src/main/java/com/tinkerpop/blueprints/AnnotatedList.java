package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.AnnotatedListQuery;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Sridhar Ramachandran (lambdazen@gmail.com)
 */
public interface AnnotatedList<V> {

    public static final Object MAKE = new Object();

    public AnnotatedValue<V> addValue(final V value, final Object... keyValues);

    public AnnotatedListQuery<V> query();

    public static Object make() {
        return MAKE;
    }

    public static class Exceptions {
        public static IllegalArgumentException providedKeyValuesMustBeAMultipleOfTwo() {
            return new IllegalArgumentException("The provided key/value array must be a multiple of two");
        }

        public static IllegalArgumentException providedKeyValuesMustHaveALegalKeyOnEvenIndices() {
            return new IllegalArgumentException("The provided key/value array must have a String key or Annotation.Key on even array indices");
        }
    }
}
