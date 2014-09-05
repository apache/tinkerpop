package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.structure.util.EmptyMetaProperty;

import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface MetaProperty<V> extends Property<V>, Element {

    public static final String META_PROPERTY = "metaProperty";
    public static final String VALUE = "value";
    public static final String KEY = "key";

    @Override
    public Vertex getElement();

    @Override
    public default String label() {
        return META_PROPERTY;
    }

    public static <V> MetaProperty<V> empty() {
        return EmptyMetaProperty.instance();
    }

    @Override
    public MetaProperty.Iterators iterators();

    public interface Iterators extends Element.Iterators {

        @Override
        public <U> Iterator<Property<U>> properties(final String... propertyKeys);

        @Override
        public <U> Iterator<Property<U>> hiddens(final String... propertyKeys);
    }

}
