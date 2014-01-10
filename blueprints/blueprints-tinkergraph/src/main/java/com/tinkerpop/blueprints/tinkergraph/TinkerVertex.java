package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Strategy;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultAnnotatedList;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerVertex extends TinkerElement implements Vertex {

    protected Map<String, Set<Edge>> outEdges = new HashMap<>();
    protected Map<String, Set<Edge>> inEdges = new HashMap<>();

    private final Strategy.Context<Vertex> strategyContext = new Strategy.Context<Vertex>(this.graph, this);

    protected TinkerVertex(final String id, final String label, final TinkerGraph graph) {
        super(id, label, graph);
        this.state = TinkerGraphComputer.State.STANDARD;
        this.centricId = id;
    }

    private TinkerVertex(final TinkerVertex vertex, final TinkerGraphComputer.State state, final String centricId, final TinkerVertexMemory annotationMemory) {
        super(vertex.id, vertex.label, vertex.graph);
        this.state = state;
        this.outEdges = vertex.outEdges;
        this.inEdges = vertex.inEdges;
        this.properties = vertex.properties;
        this.vertexMemory = annotationMemory;
        this.centricId = centricId;
    }

    public <V> void setProperty(final String key, final V value) {
        if (TinkerGraphComputer.State.STANDARD == this.state) {
            ElementHelper.validateProperty(key, value);
            final Property oldProperty = super.getProperty(key);
            if (value == AnnotatedList.make()) {
                if (!this.properties.containsKey(key) || !(this.properties.get(key) instanceof AnnotatedList))
                    this.properties.put(key, new TinkerProperty<>(this, key, new DefaultAnnotatedList<>()));
            } else
                this.properties.put(key, new TinkerProperty<>(this, key, value));
            this.graph.vertexIndex.autoUpdate(key, value, oldProperty.isPresent() ? oldProperty.get() : null, this);
        } else if (TinkerGraphComputer.State.CENTRIC == this.state) {
            ElementHelper.validateProperty(key, value);
            if (this.vertexMemory.getComputeKeys().containsKey(key))
                this.vertexMemory.setProperty(this, key, value);
            else
                throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
        } else {
            throw GraphComputer.Exceptions.adjacentElementPropertiesCanNotBeWritten();
        }
    }

    public VertexQuery query() {
        return new TinkerVertexQuery(this, this.vertexMemory);
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        // The first argument to compose() gets the GraphStrategy to use and provides it the Context of the addEdge
        // call. The second argument to compose() is the TinkerGraph implementation of addEdge as a lambda where
        // the argument refer to the arguments to addEdge. Note that arguments passes through the GraphStrategy
        // implementations first so at this point the values within them may not be the same as they originally were.
        // The composed function must then be applied with the arguments originally passed to addEdge.
        return this.graph.strategy().compose(
                s -> s.getAddEdgeStrategy(strategyContext),
                (l, v, kvs) -> TinkerHelper.addEdge(this.graph, this, (TinkerVertex) v, l, kvs))
                .apply(label, vertex, keyValues);
    }

    public void remove() {
        // The first argument to compose() gets the GraphStrategy to use and provides it the Context of the remove
        // call. The second argument to compose() is the TinkerGraph implementation of remove as a lambda where
        // the argument refer to the arguments to remove. Note that arguments passes through the GraphStrategy
        // implementations first so at this point the values within them may not be the same as they originally were.
        // The composed function must then be applied with the arguments originally passed to remove.
        this.graph.strategy().compose(
                s -> s.getRemoveVertexStrategy(strategyContext),
                () -> {
                    if (!graph.vertices.containsKey(this.id))
                        throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Vertex.class, this.id);

                    this.query().direction(Direction.BOTH).edges().forEach(Edge::remove);
                    this.getProperties().clear();
                    graph.vertexIndex.removeElement(this);
                    graph.vertices.remove(this.id);
                    return null;
                }).get();
    }

    public TinkerVertex createClone(final TinkerGraphComputer.State state, final String centricId, final TinkerVertexMemory vertexMemory) {
        return new TinkerVertex(this, state, centricId, vertexMemory);
    }
}
