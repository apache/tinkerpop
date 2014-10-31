package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.graph.VertexTraversal;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Iterator;
import java.util.Optional;

/**
 * A {@link Vertex} maintains pointers to both a set of incoming and outgoing {@link Edge} objects. The outgoing edges
 * are those edges for  which the {@link Vertex} is the tail. The incoming edges are those edges for which the
 * {@link Vertex} is the head.
 * <p/>
 * Diagrammatically:
 * <pre>
 * ---inEdges---> vertex ---outEdges--->.
 * </pre>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Vertex extends Element, VertexTraversal {

    /**
     * The default label to use for a vertex.
     */
    public static final String DEFAULT_LABEL = "vertex";

    /**
     * Add an outgoing edge to the vertex with provided label and edge properties as key/value pairs.
     * These key/values must be provided in an even number where the odd numbered arguments are {@link String}
     * property keys and the even numbered arguments are the related property values.  Hidden properties can be
     * set by specifying the key as {@link com.tinkerpop.gremlin.structure.Graph.Key#hide}.
     *
     * @param label     The label of the edge
     * @param inVertex  The vertex to receive an incoming edge from the current vertex
     * @param keyValues The key/value pairs to turn into edge properties
     * @return the newly created edge
     */
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues);

    @Override
    public default <V> VertexProperty<V> property(final String key) {
        final Iterator<VertexProperty<V>> iterator = Graph.Key.isHidden(key) ?
                this.iterators().hiddenPropertyIterator(Graph.Key.unHide(key)) :
                this.iterators().propertyIterator(key);
        if (iterator.hasNext()) {
            final VertexProperty<V> property = iterator.next();
            if (iterator.hasNext())
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            else
                return property;
        } else {
            return VertexProperty.<V>empty();
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value);

    public default <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        final Optional<Object> optionalId = ElementHelper.getIdValue(keyValues);
        if (optionalId.isPresent() && !graph().features().vertex().properties().supportsUserSuppliedIds())
            throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();

        final VertexProperty<V> vertexProperty = this.property(key, value);
        ElementHelper.attachProperties(vertexProperty, keyValues);
        return vertexProperty;
    }

    public default <V> VertexProperty<V> singleProperty(final String key, final V value, final Object... keyValues) {
        if (Graph.Key.isHidden(key))
            this.iterators().hiddenPropertyIterator(Graph.Key.unHide(key)).forEachRemaining(VertexProperty::remove);
        else
            this.iterators().propertyIterator(key).forEachRemaining(VertexProperty::remove);
        return this.property(key, value, keyValues);
    }

    /**
     * Gets the {@link Vertex.Iterators} set.
     * <p/>
     * {@inheritDoc}
     */
    @Override
    public Vertex.Iterators iterators();

    /**
     * An interface that provides access to iterators over {@link VertexProperty} objects, {@link Edge} objects
     * and adjacent vertices, associated with the {@code Vertex}, without constructing a
     * {@link com.tinkerpop.gremlin.process.Traversal} object.
     */
    public interface Iterators extends Element.Iterators {
        /**
         * Gets an {@link Iterator} of incident edges.
         *
         * @param direction  The incident direction of the edges to retrieve off this vertex
         * @param edgeLabels The labels of the edges to retrieve
         * @return An iterator of edges meeting the provided specification
         */
        public Iterator<Edge> edgeIterator(final Direction direction, final String... edgeLabels);

        /**
         * Gets an {@link Iterator} of adjacent vertices.
         *
         * @param direction  The adjacency direction of the vertices to retrieve off this vertex
         * @param edgeLabels The labels of the edges associated with the vertices to retrieve
         * @return An iterator of vertices meeting the provided specification
         */
        public Iterator<Vertex> vertexIterator(final Direction direction, final String... edgeLabels);

        /**
         * {@inheritDoc}
         */
        @Override
        public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys);

        /**
         * {@inheritDoc}
         */
        @Override
        public <V> Iterator<VertexProperty<V>> hiddenPropertyIterator(final String... propertyKeys);
    }

    /**
     * Common exceptions to use with a vertex.
     */
    public static class Exceptions {
        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Vertex does not support user supplied identifiers");
        }

        public static UnsupportedOperationException userSuppliedIdsOfThisTypeNotSupported() {
            return new UnsupportedOperationException("Vertex does not support user supplied identifiers of this type");
        }

        public static IllegalStateException vertexRemovalNotSupported() {
            return new IllegalStateException("Vertex removal are not supported");
        }

        public static IllegalStateException edgeAdditionsNotSupported() {
            return new IllegalStateException("Edge additions not supported");
        }

        public static IllegalStateException multiplePropertiesExistForProvidedKey(final String propertyKey) {
            return Graph.Key.isHidden(propertyKey) ?
                    new IllegalStateException("Multiple properties exist for the provided hidden key, use Vertex.properties(Graph.Key.hide(" + propertyKey + "))") :
                    new IllegalStateException("Multiple properties exist for the provided key, use Vertex.properties(" + propertyKey + ")");
        }
    }
}
