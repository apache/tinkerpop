package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.computer.util.GiraphComputerHelper;
import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphGraphStep;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.util.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphEdge extends GiraphElement implements Edge, Serializable, WrappedEdge<TinkerEdge> {

    protected GiraphEdge() {
    }

    public GiraphEdge(final TinkerEdge edge, final GiraphGraph graph) {
        super(edge, graph);
    }

    public Iterator<Vertex> vertices(final Direction direction) {
        return GiraphHelper.getVertices(this.graph, this, direction);
    }

    public GraphTraversal<Edge, Edge> start() {
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<Vertex, Vertex>(this.graph) {
            public GraphTraversal<Vertex, Vertex> submit(final GraphComputer computer) {
                GiraphComputerHelper.prepareTraversalForComputer(this);
                final String label = this.getSteps().get(0).getLabel();
                TraversalHelper.removeStep(0, this);
                final Step identityStep = new IdentityStep(this);
                if (TraversalHelper.isLabeled(label))
                    identityStep.setLabel(label);

                TraversalHelper.insertStep(identityStep, 0, this);
                TraversalHelper.insertStep(new HasStep(this, new HasContainer(Element.ID, Compare.EQUAL, element.id())), 0, this);
                TraversalHelper.insertStep(new GiraphGraphStep<>(this, Edge.class, graph), 0, this);

                return super.submit(computer);
            }
        };
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    public TinkerEdge getBaseEdge() {
        return (TinkerEdge) this.element;
    }
}