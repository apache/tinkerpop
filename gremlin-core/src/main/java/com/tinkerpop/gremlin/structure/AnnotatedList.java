package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Sridhar Ramachandran (lambdazen@gmail.com)
 */
public interface AnnotatedList<V> {

    public static final MakeObject MAKE = new MakeObject();

    public AnnotatedValue<V> addValue(final V value, final Object... annotationKeyValues);

    public GraphTraversal<AnnotatedList<V>, AnnotatedValue<V>> annotatedValues();

    public default GraphTraversal<AnnotatedList<V>, V> values() {
        return this.annotatedValues().value();
    }

    public static Object make() {
        return MAKE;
    }

    public static final class MakeObject {

        private MakeObject() {
        }

        public boolean equals(final Object object) {
            return object instanceof MakeObject;
        }
    }
}
