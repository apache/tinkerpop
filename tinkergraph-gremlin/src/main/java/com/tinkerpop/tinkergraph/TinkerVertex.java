package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.map.StartStep;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.tinkergraph.process.graph.map.TinkerVertexStep;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertex extends TinkerElement implements Vertex {

    protected Map<String, Set<Edge>> outEdges = new HashMap<>();
    protected Map<String, Set<Edge>> inEdges = new HashMap<>();

    protected TinkerVertex(final String id, final String label, final TinkerGraph graph) {
        super(id, label, graph);
    }

    public <V> void setProperty(final String key, final V value) {
        if (this.graph.usesElementMemory) {
            this.graph.elementMemory.setProperty(this, key, value);
        } else {
            ElementHelper.validateProperty(key, value);
            final Property oldProperty = super.getProperty(key);
            if (value == AnnotatedList.make()) {
                if (!this.properties.containsKey(key) || !(this.properties.get(key) instanceof AnnotatedList))
                    this.properties.put(key, new TinkerProperty<>(this, key, new TinkerAnnotatedList<>()));
            } else
                this.properties.put(key, new TinkerProperty<>(this, key, value));
            this.graph.vertexIndex.autoUpdate(key, value, oldProperty.isPresent() ? oldProperty.get() : null, this);
        }
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        return TinkerHelper.addEdge(this.graph, this, (TinkerVertex) vertex, label, keyValues);
    }

    public void remove() {
        if (!this.graph.vertices.containsKey(this.id))
            throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Vertex.class, this.id);

        this.bothE().forEach(Edge::remove);
        this.getProperties().clear();
        this.graph.vertexIndex.removeElement(this);
        this.graph.vertices.remove(this.id);
    }

    //////////////////////

    public GraphTraversal<Vertex, Vertex> out(final int branchFactor, final String... labels) {
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new TinkerVertexStep(traversal, Vertex.class, Direction.OUT, branchFactor, labels));
        return traversal;
    }

    public GraphTraversal<Vertex, Vertex> in(final int branchFactor, final String... labels) {
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new TinkerVertexStep(traversal, Vertex.class, Direction.IN, branchFactor, labels));
        return traversal;
    }

    public GraphTraversal<Vertex, Vertex> both(final int branchFactor, final String... labels) {
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new TinkerVertexStep(traversal, Vertex.class, Direction.BOTH, branchFactor, labels));
        return traversal;
    }

    public GraphTraversal<Vertex, Edge> outE(final int branchFactor, final String... labels) {
        final GraphTraversal<Vertex, Edge> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new TinkerVertexStep(traversal, Edge.class, Direction.OUT, branchFactor, labels));
        return traversal;
    }

    public GraphTraversal<Vertex, Edge> inE(final int branchFactor, final String... labels) {
        final GraphTraversal<Vertex, Edge> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new TinkerVertexStep(traversal, Edge.class, Direction.IN, branchFactor, labels));
        return traversal;
    }

    public GraphTraversal<Vertex, Edge> bothE(final int branchFactor, final String... labels) {
        final GraphTraversal<Vertex, Edge> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new TinkerVertexStep(traversal, Edge.class, Direction.BOTH, branchFactor, labels));
        return traversal;
    }
}
