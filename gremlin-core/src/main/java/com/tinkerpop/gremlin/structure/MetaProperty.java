package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.graph.MetaPropertyTraversal;
import com.tinkerpop.gremlin.structure.util.EmptyMetaProperty;

import java.util.Iterator;

/**
 * A {@code MetaProperty} is similar to a {@link Property} in that it denotes a key/value pair associated with an
 * {@link Vertex}, however it is different in the sense that it also represents an entity that it is an {@link Element}
 * that can have properties of its own.
 * <br/>
 * A property is much like a Java8 {@link java.util.Optional} in that a property can be not present (i.e. empty).
 * The key of a property is always a String and the value of a property is an arbitrary Java object.
 * Each underlying graph engine will typically have constraints on what Java objects are allowed to be used as values.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface MetaProperty<V> extends Property<V>, Element, MetaPropertyTraversal {

    public static final String DEFAULT_LABEL = "metaProperty";

    @Override
    public Vertex getElement();

    @Override
    public default String label() {
        return this.key();
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
