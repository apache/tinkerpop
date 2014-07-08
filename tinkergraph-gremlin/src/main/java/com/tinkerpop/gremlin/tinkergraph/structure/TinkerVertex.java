package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.filter.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.map.TinkerGraphStep;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.map.TinkerVertexStep;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertex extends TinkerElement implements Vertex {

    protected Map<String, Set<Edge>> outEdges = new HashMap<>();
    protected Map<String, Set<Edge>> inEdges = new HashMap<>();

    protected TinkerVertex(final Object id, final String label, final TinkerGraph graph) {
        super(id, label, graph);
    }

    public <V> Property<V> property(final String key, final V value) {
        if (this.graph.useGraphView) {
            return this.graph.graphView.setProperty(this, key, value);
        } else {
            ElementHelper.validateProperty(key, value);
            final Property oldProperty = super.property(key);
            final Property newProperty = new TinkerProperty<>(this, key, value);
            this.properties.put(key, newProperty);
            this.graph.vertexIndex.autoUpdate(key, value, oldProperty.isPresent() ? oldProperty.value() : null, this);
            return newProperty;
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
        this.properties().clear();
        this.graph.vertexIndex.removeElement(this);
        this.graph.vertices.remove(this.id);
    }

    //////////////////////


    public GraphTraversal<Vertex, Vertex> start() {
        final TinkerVertex vertex = this;
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<Vertex, Vertex>() {
            public GraphTraversal<Vertex, Vertex> submit(final TraversalEngine engine) {
                if (engine instanceof GraphComputer) {
                    TinkerHelper.prepareTraversalForComputer(this);
                    final String label = this.getSteps().get(0).getAs();
                    TraversalHelper.removeStep(0, this);
                    final Step identityStep = new IdentityStep(this);
                    if (TraversalHelper.isLabeled(label))
                        identityStep.setAs(label);

                    TraversalHelper.insertStep(identityStep, 0, this);
                    TraversalHelper.insertStep(new HasStep(this, new HasContainer(Element.ID, Compare.EQUAL, vertex.id())), 0, this);
                    TraversalHelper.insertStep(new TinkerGraphStep<>(this, Vertex.class, vertex.graph), 0, this);
                }
                return super.submit(engine);
            }
        };
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    public GraphTraversal<Vertex, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        final GraphTraversal<Vertex, Vertex> traversal = this.start();
        traversal.addStep(new TinkerVertexStep(traversal, Vertex.class, direction, branchFactor, labels));
        return traversal;
    }

    public GraphTraversal<Vertex, Edge> toE(final Direction direction, final int branchFactor, final String... labels) {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new TinkerVertexStep(traversal, Edge.class, direction, branchFactor, labels));
        return traversal;
    }

    public Iterator<Edge> toEIterator(final Direction direction, final int branchFactor, final String... labels) {
        return (Iterator) StreamFactory.stream(TinkerHelper.getEdges(this, direction, labels)).limit(branchFactor).iterator();
    }

    public Iterator<Vertex> toIterator(final Direction direction, final int branchFactor, final String... labels) {
        return (Iterator) StreamFactory.stream(TinkerHelper.getVertices(this, direction, labels)).limit(branchFactor).iterator();
    }
}
