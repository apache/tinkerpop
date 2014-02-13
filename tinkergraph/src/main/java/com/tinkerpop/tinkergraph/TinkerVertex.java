package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.SimpleHolder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.olap.GraphComputer;
import com.tinkerpop.gremlin.process.steps.map.IdentityStep;
import com.tinkerpop.gremlin.process.steps.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Strategy;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.query.VertexQuery;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerVertex extends TinkerElement implements Vertex {

    protected Map<String, Set<Edge>> outEdges = new HashMap<>();
    protected Map<String, Set<Edge>> inEdges = new HashMap<>();

    private transient final Strategy.Context<Vertex> strategyContext = new Strategy.Context<Vertex>(this.graph, this);

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
        // The first argument to compose() gets the GraphStrategy to use and provides it the Context of the setProperty
        // call. The second argument to compose() is the TinkerGraph implementation of setProperty as a lambda where
        // the argument refer to the arguments to setProperty. Note that arguments passes through the GraphStrategy
        // implementations first so at this point the values within them may not be the same as they originally were.
        // The composed function must then be applied with the arguments originally passed to setProperty.
        this.graph.strategy.compose(s -> s.<V>getElementSetProperty(strategyContext), (k, v) -> {
            ElementHelper.validateProperty(k, v);
            if (TinkerGraphComputer.State.STANDARD == this.state) {
                final Property oldProperty = super.getProperty(k);
                if (v == AnnotatedList.make()) {
                    if (!this.properties.containsKey(k) || !(this.properties.get(k) instanceof AnnotatedList))
                        this.properties.put(k, new TinkerProperty<>(this, k, new TinkerAnnotatedList<>()));
                } else
                    this.properties.put(k, new TinkerProperty<>(this, k, v));
                this.graph.vertexIndex.autoUpdate(k, v, oldProperty.isPresent() ? oldProperty.get() : null, this);
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
                s -> s.getRemoveElementStrategy(strategyContext),
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

    //////////////////////

    public <A extends Traversal<Vertex, Vertex>> A out(final String... labels) {
        final DefaultTraversal<Vertex, Vertex> traversal = new DefaultTraversal<>();
        traversal.addStep(new IdentityStep<>(traversal));
        traversal.addStarts(new SingleIterator<Holder<Vertex>>(new SimpleHolder<Vertex>(this)));
        return (A) traversal.out(labels);
    }

    public <A extends Traversal<Vertex, Vertex>> A in(final String... labels) {
        final DefaultTraversal<Vertex, Vertex> traversal = new DefaultTraversal<>();
        traversal.addStep(new IdentityStep<>(traversal));
        traversal.addStarts(new SingleIterator<Holder<Vertex>>(new SimpleHolder<Vertex>(this)));
        return (A) traversal.in(labels);
    }

    public <A extends Traversal<Vertex, Vertex>> A both(final String... labels) {
        final DefaultTraversal<Vertex, Vertex> traversal = new DefaultTraversal<>();
        traversal.addStep(new IdentityStep<>(traversal));
        traversal.addStarts(new SingleIterator<Holder<Vertex>>(new SimpleHolder<Vertex>(this)));
        return (A) traversal.both(labels);
    }
}
