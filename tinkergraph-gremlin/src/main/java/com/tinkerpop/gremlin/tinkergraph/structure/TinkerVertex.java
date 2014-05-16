package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.map.IdentityStep;
import com.tinkerpop.gremlin.process.graph.map.StartStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.tinkergraph.process.graph.map.TinkerGraphStep;
import com.tinkerpop.gremlin.tinkergraph.process.graph.map.TinkerVertexStep;
import com.tinkerpop.gremlin.tinkergraph.process.graph.util.optimizers.TinkerGraphStepTraversalStrategy;

import java.util.HashMap;
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
            Property newProperty;
            if (value == AnnotatedList.make()) {
                if (!this.properties.containsKey(key) || !(this.properties.get(key) instanceof AnnotatedList)) {
                    newProperty = new TinkerProperty<>(this, key, new TinkerAnnotatedList<>());
                    this.properties.put(key, newProperty);
                } else {
                    return this.properties.get(key);
                }
            } else {
                newProperty = new TinkerProperty<>(this, key, value);
                this.properties.put(key, newProperty);
            }
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
                    this.optimizers().unregister(TinkerGraphStepTraversalStrategy.class);
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

    public GraphTraversal<Vertex, Vertex> out(final int branchFactor, final String... labels) {
        final GraphTraversal<Vertex, Vertex> traversal = this.start();
        traversal.addStep(new TinkerVertexStep(traversal, Vertex.class, Direction.OUT, branchFactor, labels));
        return traversal;
    }

    public GraphTraversal<Vertex, Vertex> in(final int branchFactor, final String... labels) {
        final GraphTraversal<Vertex, Vertex> traversal = this.start();
        traversal.addStep(new TinkerVertexStep(traversal, Vertex.class, Direction.IN, branchFactor, labels));
        return traversal;
    }

    public GraphTraversal<Vertex, Vertex> both(final int branchFactor, final String... labels) {
        final GraphTraversal<Vertex, Vertex> traversal = this.start();
        traversal.addStep(new TinkerVertexStep(traversal, Vertex.class, Direction.BOTH, branchFactor, labels));
        return traversal;
    }

    public GraphTraversal<Vertex, Edge> outE(final int branchFactor, final String... labels) {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new TinkerVertexStep(traversal, Edge.class, Direction.OUT, branchFactor, labels));
        return traversal;
    }

    public GraphTraversal<Vertex, Edge> inE(final int branchFactor, final String... labels) {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new TinkerVertexStep(traversal, Edge.class, Direction.IN, branchFactor, labels));
        return traversal;
    }

    public GraphTraversal<Vertex, Edge> bothE(final int branchFactor, final String... labels) {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new TinkerVertexStep(traversal, Edge.class, Direction.BOTH, branchFactor, labels));
        return traversal;
    }
}
