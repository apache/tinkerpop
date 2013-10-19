package com.tinkerpop.blueprints.tinkergraph;


import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.GraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.ThingHelper;
import org.apache.commons.configuration.Configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * An in-memory, reference implementation of the property graph interfaces provided by Blueprints.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraph implements Graph, Serializable {

    protected Long currentId = -1l;
    protected Map<String, Vertex> vertices = new HashMap<>();
    protected Map<String, Edge> edges = new HashMap<>();
    protected Map<String, Property> properties = new HashMap<>();

    protected TinkerIndex<TinkerVertex> vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
    protected TinkerIndex<TinkerEdge> edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);

    public TinkerGraph() {
    }

    public TinkerGraph(final Configuration configuration) {
        if (configuration == null) {
            throw new IllegalArgumentException("configuration cannot be null");
        }

        // for future use and compatibility with TinkerPop2...........
        //this.directory = configuration.getString("blueprints.tg.directory", null);
        //this.fileType = FileType.valueOf(configuration.getString("blueprints.tg.file-type", "JAVA"));
        /*
        if (directory != null) {
            this.init();
        }
        */
    }

    ////////////// BLUEPRINTS API METHODS //////////////////

    public Vertex addVertex(final Property... properties) {
        Objects.requireNonNull(properties);
        String idString = Stream.of(properties)
                .filter(p -> p.is(Property.Key.ID))
                .map(p -> p.getValue().toString())
                .findFirst()
                .orElse(null);

        if (null != idString) {
            if (this.vertices.containsKey(idString))
                throw ExceptionFactory.vertexWithIdAlreadyExists(idString);
        } else {
            idString = TinkerHelper.getNextId(this);
        }

        final Vertex vertex = new TinkerVertex(idString, this);
        this.vertices.put(vertex.getId().toString(), vertex);
        Stream.of(properties)
                .filter(p -> p.isPresent() & !p.is(Property.Key.ID))
                .forEach(p -> {
                    vertex.setProperty(p.getKey(), p.getValue());
                });
        return vertex;

    }

    public GraphQuery query() {
        return new TinkerGraphQuery(this);
    }

    public GraphComputer compute() {
        return new TinkerGraphComputer(this);
    }

    public <T> Property<T, Graph> getProperty(final String key) {
        final Property<T, Graph> property = this.properties.get(key);
        return (null == property) ? Property.empty() : property;
    }

    public <T> Property<T, Graph> setProperty(final String key, final T value) {
        ThingHelper.validateProperty(this, key, value);
        final Property<T, Graph> property = this.properties.put(key, new TinkerProperty<>(key, value, this));
        return null == property ? Property.empty() : property;
    }

    public <T> Property<T, Graph> removeProperty(final String key) {
        final Property<T, Graph> property = this.properties.remove(key);
        return null == property ? Property.empty() : property;
    }


    public String toString() {
        return StringFactory.graphString(this, "vertices:" + this.vertices.size() + " edges:" + this.edges.size());
    }

    public void clear() {
        this.vertices.clear();
        this.edges.clear();
        this.currentId = 0l;
        this.vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
        this.edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);
    }

    public void close() {

    }

    public void commit() {

    }

    public void rollback() {

    }


    public Features getFeatures() {
        return null;
    }

    ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

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
}
