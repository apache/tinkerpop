package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.graph.VertexPropertyTraversal;
import com.tinkerpop.gremlin.structure.util.empty.EmptyVertexProperty;

import java.util.Iterator;

/**
 * A {@code VertexProperty} is similar to a {@link Property} in that it denotes a key/value pair associated with an
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
public interface VertexProperty<V> extends Property<V>, Element, VertexPropertyTraversal {

    public static final String DEFAULT_LABEL = "vertexProperty";

    /**
     * Gets the {@link Vertex} that owns this {@code VertexProperty}.
     */
    @Override
    public Vertex element();

    /**
     * {@inheritDoc}
     */
    @Override
    @Graph.Helper
    public default Graph graph() {
        return this.element().graph();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Graph.Helper
    public default String label() {
        return this.key();
    }

    /**
     * Constructs an empty {@code VertexProperty}.
     */
    public static <V> VertexProperty<V> empty() {
        return EmptyVertexProperty.instance();
    }

    /**
     * Gets the {@link VertexProperty.Iterators} set.
     * <p/>
     * {@inheritDoc}
     */
    @Override
    public VertexProperty.Iterators iterators();

    /**
     * An interface that provides access to iterators over properties, without constructing a
     * {@link com.tinkerpop.gremlin.process.Traversal} object.
     */
    public interface Iterators extends Element.Iterators {
        /**
         * {@inheritDoc}
         */
        @Override
        public <U> Iterator<Property<U>> propertyIterator(final String... propertyKeys);
    }

    /**
     * Common exceptions to use with a property.
     */
    public static class Exceptions {
        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("VertexProperty does not support user supplied identifiers");
        }

        public static UnsupportedOperationException userSuppliedIdsOfThisTypeNotSupported() {
            return new UnsupportedOperationException("VertexProperty does not support user supplied identifiers of this type");
        }

        public static UnsupportedOperationException multiPropertiesNotSupported() {
            return new UnsupportedOperationException("Multiple properties on a vertex is not supported");
        }

        public static UnsupportedOperationException metaPropertiesNotSupported() {
            return new UnsupportedOperationException("Properties on a vertex property is not supported");
        }
    }
}
