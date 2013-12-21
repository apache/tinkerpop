package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.VertexQuery;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StringFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerVertex extends TinkerElement implements Vertex, Serializable {

    protected enum State {STANDARD, CENTRIC, ADJACENT}

    protected TinkerAnnotationMemory annotationMemory;
    protected final State state;
    protected String centricId;

    protected Map<String, Set<Edge>> outEdges = new HashMap<>();
    protected Map<String, Set<Edge>> inEdges = new HashMap<>();

    protected TinkerVertex(final String id, final String label, final TinkerGraph graph) {
        super(id, label, graph);
        this.state = State.STANDARD;
        this.centricId = id;
    }

    protected TinkerVertex(final TinkerVertex vertex, final State state, final String centricId, final TinkerAnnotationMemory annotationMemory) {
        super(vertex.id, vertex.label, vertex.graph);
        this.state = state;
        this.outEdges = vertex.outEdges;
        this.inEdges = vertex.inEdges;
        this.properties = vertex.properties;
        this.annotationMemory = annotationMemory;
        this.centricId = centricId;
    }

    public <V> void setAnnotation(final String key, final V value) {
        if (this.state == State.STANDARD) {
            this.setAnnotation(key, value);
        } else if (this.state == State.CENTRIC) {
            if (this.annotationMemory.isComputeKey(key))
                this.annotationMemory.setElementAnnotation(this, key, value);
            else
                throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
        } else {
            throw GraphComputer.Exceptions.adjacentVertexAnnotationsCanNotBeWritten();
        }
    }

    public <V> Optional<V> getAnnotation(final String key) {
        if (this.state == State.STANDARD) {
            return super.getAnnotation(key);
        } else if (this.state == State.CENTRIC) {
            if (this.annotationMemory.isComputeKey(key))
                return this.annotationMemory.getElementAnnotation(this, key);
            else
                return super.getAnnotation(key);
        } else {
            throw GraphComputer.Exceptions.adjacentVertexAnnotationsCanNotBeRead();
        }
    }

    public <V> Property<V> getProperty(final String key) {
        if (this.state != State.ADJACENT) {
            return super.getProperty(key);
        } else {
            throw GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeRead();
        }
    }

    public <V> void setProperty(final String key, final V value) {
        if (this.state != State.ADJACENT) {
            ElementHelper.validateProperty(key, value);
            final TinkerVertex vertex = this;
            final Property oldProperty = super.getProperty(key);
            this.properties.put(key, new TinkerProperty<V>(this, key, value) {
                @Override
                public void remove() {
                    vertex.properties.remove(key);
                }

                public <E extends Element> E getElement() {
                    return (E) vertex;
                }
            });
            this.graph.vertexIndex.autoUpdate(key, value, oldProperty.isPresent() ? oldProperty.getValue() : null, this);
        } else {
            GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeWritten();
        }
    }

    public VertexQuery query() {
        return new TinkerVertexQuery(this, this.annotationMemory);
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        return TinkerHelper.addEdge(this.graph, this, (TinkerVertex) vertex, label, keyValues);
    }

    public void remove() {
        if (!graph.vertices.containsKey(this.id))
            throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Vertex.class, this.getId());

        this.query().direction(Direction.BOTH).edges().forEach(Edge::remove);
        this.properties.clear();
        this.annotations.clear();
        graph.vertexIndex.removeElement(this);
        graph.vertices.remove(this.id);
    }

    public TinkerVertex createClone(final State state, final String centricId, final TinkerAnnotationMemory annotationMemory) {
        return new TinkerVertex(this, state, centricId, annotationMemory);
    }
}
