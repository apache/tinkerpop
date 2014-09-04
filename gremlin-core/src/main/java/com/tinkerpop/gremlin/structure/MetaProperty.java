package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.structure.util.EmptyMetaProperty;

import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface MetaProperty<V> extends Property<V>, Element {

    public static final String META_PROPERTY = "metaProperty";

    public Vertex getElement();

    public default String label() {
        return META_PROPERTY;
    }

    public MetaProperty.Iterators iterators();

    public static <V> MetaProperty<V> empty() {
        return new EmptyMetaProperty<>();
    }

    public interface Iterators extends Element.Iterators {

        public <U> Iterator<Property<U>> properties(final String... propertyKeys);

        public <U> Iterator<Property<U>> hiddens(final String... propertyKeys);
    }

}
