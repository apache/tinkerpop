package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.SimpleHolder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.olap.GraphComputer;
import com.tinkerpop.gremlin.process.steps.map.StartStep;
import com.tinkerpop.gremlin.process.steps.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.strategy.Strategy;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.tinkergraph.process.steps.map.TinkerVertexStep;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertex extends TinkerElement implements Vertex {

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
        ElementHelper.validateProperty(key, value);
        if (TinkerGraphComputer.State.STANDARD == this.state) {
            final Property oldProperty = super.getProperty(key);
            if (value == AnnotatedList.make()) {
                if (!this.properties.containsKey(key) || !(this.properties.get(key) instanceof AnnotatedList))
                    this.properties.put(key, new TinkerProperty<>(this, key, new TinkerAnnotatedList<>()));
            } else
                this.properties.put(key, new TinkerProperty<>(this, key, value));
            this.graph.vertexIndex.autoUpdate(key, value, oldProperty.isPresent() ? oldProperty.get() : null, this);
        } else if (TinkerGraphComputer.State.CENTRIC == this.state) {
            if (this.vertexMemory.getComputeKeys().containsKey(key))
                this.vertexMemory.setProperty(this, key, value);
            else
                throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
        } else {
            throw GraphComputer.Exceptions.adjacentElementPropertiesCanNotBeWritten();
        }
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        return TinkerHelper.addEdge(this.graph, this, (TinkerVertex) vertex, label, keyValues);
    }

    public void remove() {
        if (!graph.vertices.containsKey(this.id))
            throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Vertex.class, this.id);

        this.bothE().forEach(Edge::remove);
        this.getProperties().clear();
        graph.vertexIndex.removeElement(this);
        graph.vertices.remove(this.id);
    }

    public TinkerVertex createClone(final TinkerGraphComputer.State state, final String centricId, final TinkerVertexMemory vertexMemory) {
        return new TinkerVertex(this, state, centricId, vertexMemory);
    }

    //////////////////////

    public Traversal<Vertex, Vertex> out(final int branchFactor, final String... labels) {
        final DefaultTraversal<Vertex, Vertex> traversal = new DefaultTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new TinkerVertexStep(traversal, Vertex.class, Direction.OUT, branchFactor, labels));
        return traversal;
    }

    public Traversal<Vertex, Vertex> in(final int branchFactor, final String... labels) {
        final DefaultTraversal<Vertex, Vertex> traversal = new DefaultTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new TinkerVertexStep(traversal, Vertex.class, Direction.IN, branchFactor, labels));
        return traversal;
    }

    public Traversal<Vertex, Vertex> both(final int branchFactor, final String... labels) {
        final DefaultTraversal<Vertex, Vertex> traversal = new DefaultTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new TinkerVertexStep(traversal, Vertex.class, Direction.BOTH, branchFactor, labels));
        return traversal;
    }

    public Traversal<Vertex, Edge> outE(final int branchFactor, final String... labels) {
        final DefaultTraversal<Vertex, Edge> traversal = new DefaultTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new TinkerVertexStep(traversal, Edge.class, Direction.OUT, branchFactor, labels));
        traversal.addStarts(new SingleIterator<Holder<Vertex>>(new SimpleHolder<Vertex>(this)));
        return traversal;
    }

    public Traversal<Vertex, Edge> inE(final int branchFactor, final String... labels) {
        final DefaultTraversal<Vertex, Edge> traversal = new DefaultTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new TinkerVertexStep(traversal, Edge.class, Direction.IN, branchFactor, labels));
        traversal.addStarts(new SingleIterator<Holder<Vertex>>(new SimpleHolder<Vertex>(this)));
        return traversal;
    }

    public Traversal<Vertex, Edge> bothE(final int branchFactor, final String... labels) {
        final DefaultTraversal<Vertex, Edge> traversal = new DefaultTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new TinkerVertexStep(traversal, Edge.class, Direction.BOTH, branchFactor, labels));
        traversal.addStarts(new SingleIterator<Holder<Vertex>>(new SimpleHolder<Vertex>(this)));
        return traversal;
    }
}
