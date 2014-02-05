package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Strategy;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.process.olap.GraphComputer;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerEdge extends TinkerElement implements Edge {

    private final Vertex inVertex;
    private final Vertex outVertex;

    private transient final Strategy.Context<Edge> strategyContext = new Strategy.Context<Edge>(this.graph, this);

    protected TinkerEdge(final String id, final Vertex outVertex, final String label, final Vertex inVertex, final TinkerGraph graph) {
        super(id, label, graph);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.graph.edgeIndex.autoUpdate(Element.LABEL, this.label, null, this);
    }

    private TinkerEdge(final TinkerEdge edge, final TinkerGraphComputer.State state, final String centricId, final TinkerVertexMemory vertexMemory) {
        super(edge.id, edge.label, edge.graph);
        this.state = state;
        this.inVertex = edge.inVertex;
        this.outVertex = edge.outVertex;
        this.properties = edge.properties;
        this.vertexMemory = vertexMemory;
        this.centricId = centricId;
    }

    public <V> void setProperty(final String key, final V value) {
        // The first argument to compose() gets the GraphStrategy to use and provides it the Context of the setProperty
        // call. The second argument to compose() is the TinkerGraph implementation of setProperty as a lambda where
        // the argument refer to the arguments to setProperty. Note that arguments passes through the GraphStrategy
        // implementations first so at this point the values within them may not be the same as they originally were.
        // The composed function must then be applied with the arguments originally passed to setProperty.
        this.graph.strategy.compose(s -> s.<V>getElementSetProperty(strategyContext), (k,v) -> {
            ElementHelper.validateProperty(k, v);
            if (TinkerGraphComputer.State.STANDARD == this.state) {
                final Property oldProperty = super.getProperty(key);
                this.properties.put(k, new TinkerProperty<>(this, k, v));
                this.graph.edgeIndex.autoUpdate(k, v, oldProperty.isPresent() ? oldProperty.get() : null, this);
            } else if (TinkerGraphComputer.State.CENTRIC == this.state) {
                if (this.vertexMemory.getComputeKeys().containsKey(key))
                    this.vertexMemory.setProperty(this, k, v);
                else
                    throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(k);
            } else {
                throw GraphComputer.Exceptions.adjacentElementPropertiesCanNotBeWritten();
            }
        }).accept(key, value);
    }

    public Vertex getVertex(final Direction direction) throws IllegalArgumentException {
        if (direction.equals(Direction.IN))
            return this.inVertex;
        else if (direction.equals(Direction.OUT))
            return this.outVertex;
        else
            throw Element.Exceptions.bothIsNotSupported();
    }

    public void remove() {
        // The first argument to compose() gets the GraphStrategy to use and provides it the Context of the remove
        // call. The second argument to compose() is the TinkerGraph implementation of remove as a lambda where
        // the argument refer to the arguments to remove. Note that arguments passes through the GraphStrategy
        // implementations first so at this point the values within them may not be the same as they originally were.
        // The composed function must then be applied with the arguments originally passed to remove.
        this.graph.strategy().compose(
                s -> s.getRemoveElementStrategy(strategyContext),
                () -> {
                    if (!this.graph.edges.containsKey(this.getId()))
                        throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Edge.class, this.getId());

                    final TinkerVertex outVertex = (TinkerVertex) this.getVertex(Direction.OUT);
                    final TinkerVertex inVertex = (TinkerVertex) this.getVertex(Direction.IN);
                    if (null != outVertex && null != outVertex.outEdges) {
                        final Set<Edge> edges = outVertex.outEdges.get(this.getLabel());
                        if (null != edges)
                            edges.remove(this);
                    }
                    if (null != inVertex && null != inVertex.inEdges) {
                        final Set<Edge> edges = inVertex.inEdges.get(this.getLabel());
                        if (null != edges)
                            edges.remove(this);
                    }

                    this.graph.edgeIndex.removeElement(this);
                    this.graph.edges.remove(this.getId());
                    this.properties.clear();
                    return null;
                }).get();
    }

    public TinkerEdge createClone(final TinkerGraphComputer.State state, final String centricId, final TinkerVertexMemory vertexMemory) {
        return new TinkerEdge(this, state, centricId, vertexMemory);
    }

    public String toString() {
        return StringFactory.edgeString(this);

    }
}
