package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.structure.util.EmptyMetaProperty;

import java.util.Iterator;
import java.util.Map;

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

    public <U> Iterator<Property<U>> hiddens(final String... propertyKeys);

    public default Map<String, Object> values() {
        return (Map) Element.super.values();
    }

    public default Map<String, Object> hiddenValues() {
        return (Map) Element.super.hiddenValues();
    }

    public static <V> MetaProperty<V> empty() {
        return new EmptyMetaProperty<>();
    }

}
