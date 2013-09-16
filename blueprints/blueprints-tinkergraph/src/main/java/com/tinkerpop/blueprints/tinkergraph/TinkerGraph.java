package com.tinkerpop.blueprints.tinkergraph;


import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.IndexHelper;
import com.tinkerpop.blueprints.util.StringFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * An in-memory, reference implementation of the property graph interfaces provided by Blueprints.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraph implements Graph, Serializable {

    protected Long currentId = 0l;
    protected Map<String, Vertex> vertices = new HashMap<String, Vertex>();
    protected Map<String, Edge> edges = new HashMap<String, Edge>();

    protected TinkerKeyIndex<TinkerVertex> vertexIndex = new TinkerKeyIndex<TinkerVertex>(TinkerVertex.class, this);
    protected TinkerKeyIndex<TinkerEdge> edgeIndex = new TinkerKeyIndex<TinkerEdge>(TinkerEdge.class, this);

    private final String directory;
    //private final FileType fileType;

    private static final Features FEATURES = new Features();
    //private static final Features PERSISTENT_FEATURES;

    static {
        /*FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.supportsSerializableObjectProperty = true;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = true;
        FEATURES.supportsUniformListProperty = true;
        FEATURES.supportsMixedListProperty = true;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = true;
        FEATURES.supportsStringProperty = true;

        FEATURES.ignoresSuppliedIds = false;
        FEATURES.isPersistent = false;
        FEATURES.isWrapper = false;

        FEATURES.supportsIndices = true;
        FEATURES.supportsKeyIndices = true;
        FEATURES.supportsVertexKeyIndex = true;
        FEATURES.supportsEdgeKeyIndex = true;
        FEATURES.supportsVertexIndex = true;
        FEATURES.supportsEdgeIndex = true;
        FEATURES.supportsTransactions = false;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsThreadedTransactions = false;

        PERSISTENT_FEATURES = FEATURES.copyFeatures();
        PERSISTENT_FEATURES.isPersistent = true;*/
    }

    public enum FileType {
        JAVA,
        GML,
        GRAPHML,
        GRAPHSON
    }

    public TinkerGraph() {
        this.directory = null;
        //this.fileType = FileType.JAVA;
    }

    public <T extends Element> void createIndex(final String key, final Class<T> elementClass) {
        if (elementClass == null)
            throw ExceptionFactory.classForElementCannotBeNull();

        if (Vertex.class.isAssignableFrom(elementClass)) {
            this.vertexIndex.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            this.edgeIndex.createKeyIndex(key);
        } else {
            throw ExceptionFactory.classIsNotIndexable(elementClass);
        }
    }

    public <T extends Element> void dropIndex(final String key, final Class<T> elementClass) {
        if (elementClass == null)
            throw ExceptionFactory.classForElementCannotBeNull();

        if (Vertex.class.isAssignableFrom(elementClass)) {
            this.vertexIndex.dropKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            this.edgeIndex.dropKeyIndex(key);
        } else {
            throw ExceptionFactory.classIsNotIndexable(elementClass);
        }
    }

    public <T extends Element> Set<String> getIndexedKeys(final Class<T> elementClass) {
        if (elementClass == null)
            throw ExceptionFactory.classForElementCannotBeNull();

        if (Vertex.class.isAssignableFrom(elementClass)) {
            return this.vertexIndex.getIndexedKeys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return this.edgeIndex.getIndexedKeys();
        } else {
            throw ExceptionFactory.classIsNotIndexable(elementClass);
        }
    }


    public Vertex addVertex(final Property... properties) {
        String idString = Stream.of(properties)
                .filter((Property p) -> p.getKey().equals(Property.Key.ID.toString()))
                .map(p -> p.getValue().toString())
                .findFirst()
                .orElseGet(() -> null);

        if (null != idString) {
            if (this.vertices.containsKey(idString)) {
                throw ExceptionFactory.vertexWithIdAlreadyExists(idString);
            }
        } else {
            boolean done = false;
            while (!done) {
                idString = this.getNextId();
                if (!this.vertices.containsKey(idString))
                    done = true;
            }
        }

        final Vertex vertex = new TinkerVertex(idString, this);
        this.vertices.put(vertex.getId().toString(), vertex);
        Stream.of(properties).forEach(p -> {
            vertex.setProperty(p.getKey(), p.getValue());
        });
        return vertex;

    }

    protected Edge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {
        if (label == null)
            throw ExceptionFactory.edgeLabelCanNotBeNull();

        String idString = null;
        Edge edge;
        if (null != id) {
            idString = id.toString();
            edge = this.edges.get(idString);
            if (null != edge) {
                throw ExceptionFactory.edgeWithIdAlreadyExist(id);
            }
        } else {
            boolean done = false;
            while (!done) {
                idString = this.getNextId();
                edge = this.edges.get(idString);
                if (null == edge)
                    done = true;
            }
        }

        edge = new TinkerEdge(idString, outVertex, inVertex, label, this);
        this.edges.put(edge.getId().toString(), edge);
        final TinkerVertex out = (TinkerVertex) outVertex;
        final TinkerVertex in = (TinkerVertex) inVertex;
        out.addOutEdge(label, edge);
        in.addInEdge(label, edge);
        return edge;

    }


    public GraphQuery query() {
        return new TinkerGraphQuery(this);
    }


    public String toString() {
        if (null == this.directory)
            return StringFactory.graphString(this, "vertices:" + this.vertices.size() + " edges:" + this.edges.size());
        else
            return StringFactory.graphString(this, "vertices:" + this.vertices.size() + " edges:" + this.edges.size() + " directory:" + this.directory);
    }

    public void clear() {
        this.vertices.clear();
        this.edges.clear();
        this.currentId = 0l;
        this.vertexIndex = new TinkerKeyIndex<TinkerVertex>(TinkerVertex.class, this);
        this.edgeIndex = new TinkerKeyIndex<TinkerEdge>(TinkerEdge.class, this);
    }

    public void close() {

    }

    public void commit() {

    }

    public void rollback() {

    }

    private String getNextId() {
        String idString;
        while (true) {
            idString = this.currentId.toString();
            this.currentId++;
            if (null == this.vertices.get(idString) || null == this.edges.get(idString) || this.currentId == Long.MAX_VALUE)
                break;
        }
        return idString;
    }

    public Features getFeatures() {
        return null;
    }

    protected class TinkerKeyIndex<T extends TinkerElement> extends TinkerIndex<T> implements Serializable {

        private final Set<String> indexedKeys = new HashSet<String>();
        private TinkerGraph graph;

        public TinkerKeyIndex(final Class<T> indexClass, final TinkerGraph graph) {
            super(null, indexClass);
            this.graph = graph;
        }

        public void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
            if (this.indexedKeys.contains(key)) {
                if (oldValue != null)
                    this.remove(key, oldValue, element);
                this.put(key, newValue, element);
            }
        }

        public void autoRemove(final String key, final Object oldValue, final T element) {
            if (this.indexedKeys.contains(key)) {
                this.remove(key, oldValue, element);
            }
        }

        public void createKeyIndex(final String key) {
            if (this.indexedKeys.contains(key))
                return;

            this.indexedKeys.add(key);

            if (TinkerVertex.class.equals(this.indexClass)) {
                IndexHelper.reIndexElements(graph, graph.query().vertices(), new HashSet<String>(Arrays.asList(key)));
            } else {
                IndexHelper.reIndexElements(graph, graph.query().edges(), new HashSet<String>(Arrays.asList(key)));
            }
        }

        public void dropKeyIndex(final String key) {
            if (!this.indexedKeys.contains(key))
                return;

            this.indexedKeys.remove(key);
            this.index.remove(key);

        }

        public Set<String> getIndexedKeys() {
            return new HashSet<String>(this.indexedKeys);
        }
    }

}
