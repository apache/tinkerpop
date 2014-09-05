package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.VertexTraversal;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Iterator;

/**
 * A {@link Vertex} maintains pointers to both a set of incoming and outgoing {@link Edge} objects. The outgoing edges
 * are those edges for  which the {@link Vertex} is the tail. The incoming edges are those edges for which the
 * {@link Vertex} is the head.
 * <p>
 * Diagrammatically:
 * <pre>
 * ---inEdges---> vertex ---outEdges--->.
 * </pre>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
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

    public default <V> MetaProperty<V> property(final String key) {
        final Iterator<MetaProperty<V>> iterator = this.iterators().properties(key);
        if (iterator.hasNext()) {
            final MetaProperty<V> property = iterator.next();
            if (iterator.hasNext())
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            else
                return property;
        } else {
            return MetaProperty.<V>empty();
        }
    }

    public <V> MetaProperty<V> property(final String key, final V value);

    public default <V> MetaProperty<V> property(final String key, final V value, final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        final MetaProperty<V> property = this.property(key, value);
        ElementHelper.attachProperties(property, keyValues);
        return property;
    }

    public default <V> MetaProperty<V> singleProperty(final String key, final V value, final Object... keyValues) {
        this.iterators().properties(key).forEachRemaining(MetaProperty::remove);
        return this.property(key, value, keyValues);
    }

    public Vertex.Iterators iterators();

    public interface Iterators extends Element.Iterators {
        /**
         * @param direction    The incident direction of the edges to retrieve off this vertex
         * @param branchFactor The max number of edges to retrieve
         * @param labels       The labels of the edges to retrieve
         * @return An iterator of edges meeting the provided specification
         */
        public Iterator<Edge> edges(final Direction direction, final int branchFactor, final String... labels);

        /**
         * @param direction    The adjacency direction of the vertices to retrieve off this vertex
         * @param branchFactor The max number of vertices to retrieve
         * @param labels       The labels of the edges associated with the vertices to retrieve
         * @return An iterator of vertices meeting the provided specification
         */
        public Iterator<Vertex> vertices(final Direction direction, final int branchFactor, final String... labels);

        public <V> Iterator<MetaProperty<V>> properties(final String... propertyKeys);

        public <V> Iterator<MetaProperty<V>> hiddens(final String... propertyKeys);
    }

    /**
     * Common exceptions to use with a vertex.
     */
    public static class Exceptions {
        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Vertex does not support user supplied identifiers");
        }

        public static IllegalStateException vertexRemovalNotSupported() {
            return new IllegalStateException("Vertex removal are not supported");
        }

        public static IllegalStateException edgeAdditionsNotSupported() {
            return new IllegalStateException("Edge additions not supported");
        }

        public static IllegalStateException multiplePropertiesExistForProvidedKey(final String propertyKey) {
            return new IllegalStateException("Multiple properties exist for the provided key, use Vertex.properties(" + propertyKey + ")");
        }
    }
}
