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
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.map.TinkerEdgeVertexStep;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.map.TinkerGraphStep;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerEdge extends TinkerElement implements Edge {

    protected final Vertex inVertex;
    protected final Vertex outVertex;

    protected TinkerEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex, final TinkerGraph graph) {
        super(id, label, graph);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.graph.edgeIndex.autoUpdate(Element.LABEL, this.label, null, this);
    }

    public <V> Property<V> property(final String key, final V value) {
        if (this.graph.useGraphView) {
            return this.graph.graphView.setProperty(this, key, value);
        } else {
            ElementHelper.validateProperty(key, value);
            final Property oldProperty = super.property(key);
            final Property newProperty = new TinkerProperty<>(this, key, value);
            this.properties.put(key, newProperty);
            this.graph.edgeIndex.autoUpdate(key, value, oldProperty.isPresent() ? oldProperty.value() : null, this);
            return newProperty;
        }
    }

    public GraphTraversal<Edge, Vertex> toV(final Direction direction) {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new TinkerEdgeVertexStep(traversal, direction));
        return traversal;
    }

    public void remove() {
        if (!this.graph.edges.containsKey(this.id()))
            throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Edge.class, this.id());

        final TinkerVertex outVertex = (TinkerVertex) this.outVertex;
        final TinkerVertex inVertex = (TinkerVertex) this.inVertex;

        if (null != outVertex && null != outVertex.outEdges) {
            final Set<Edge> edges = outVertex.outEdges.get(this.label());
            if (null != edges)
                edges.remove(this);
        }
        if (null != inVertex && null != inVertex.inEdges) {
            final Set<Edge> edges = inVertex.inEdges.get(this.label());
            if (null != edges)
                edges.remove(this);
        }

        this.graph.edgeIndex.removeElement(this);
        this.graph.edges.remove(this.id());
        this.properties.clear();
    }

    public String toString() {
        return StringFactory.edgeString(this);

    }

    //////

    public GraphTraversal<Edge, Edge> start() {
        final TinkerEdge edge = this;
        final GraphTraversal<Edge, Edge> traversal = new DefaultGraphTraversal<Edge, Edge>() {
            public GraphTraversal<Edge, Edge> submit(final TraversalEngine engine) {
                if (engine instanceof GraphComputer) {
                    TinkerHelper.prepareTraversalForComputer(this);
                    final String label = this.getSteps().get(0).getAs();
                    TraversalHelper.removeStep(0, this);
                    final Step identityStep = new IdentityStep(this);
                    if (TraversalHelper.isLabeled(label))
                        identityStep.setAs(label);

                    TraversalHelper.insertStep(identityStep, 0, this);
                    TraversalHelper.insertStep(new HasStep(this, new HasContainer(Element.ID, Compare.EQUAL, edge.id())), 0, this);
                    TraversalHelper.insertStep(new TinkerGraphStep<>(this, Edge.class, edge.graph), 0, this);
                }
                return super.submit(engine);
            }
        };
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }
}
