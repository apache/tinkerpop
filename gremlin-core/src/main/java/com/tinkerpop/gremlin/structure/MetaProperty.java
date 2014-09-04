package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.structure.util.EmptyMetaProperty;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface MetaProperty<V> extends Property<V>, Element {

    public static final String META_PROPERTY = "metaProperty";

    public Vertex getElement();

    public default String label() {
        return META_PROPERTY;
    }

    public <U> Iterator<Property<U>> properties(final String... propertyKeys);

    public default <U> Iterator<Property<U>> hiddens(final String... propertyKeys) {
        return (Iterator) Element.super.hiddens(propertyKeys);
    }

    public static <V> MetaProperty<V> empty() {
        return new EmptyMetaProperty<>();
    }

}
