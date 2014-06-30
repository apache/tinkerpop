package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.graph.step.map.GiraphEdgeVertexStep;
import com.tinkerpop.gremlin.giraph.process.graph.step.map.GiraphGraphStep;
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
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphEdge extends GiraphElement implements Edge, Serializable {

    protected GiraphEdge() {
    }

    public GiraphEdge(final TinkerEdge edge, final GiraphGraph graph) {
        super(edge, graph);
    }

    public GraphTraversal<Edge, Vertex> toV(final Direction direction) {
        final GraphTraversal traversal = this.start();
        return (GraphTraversal) traversal.addStep(new GiraphEdgeVertexStep(traversal, this.graph, direction));

    }

    public GraphTraversal<Edge, Edge> start() {
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<Vertex, Vertex>() {
            public GraphTraversal<Vertex, Vertex> submit(final TraversalEngine engine) {
                if (engine instanceof GraphComputer) {
                    final String label = this.getSteps().get(0).getAs();
                    TraversalHelper.removeStep(0, this);
                    final Step identityStep = new IdentityStep(this);
                    if (TraversalHelper.isLabeled(label))
                        identityStep.setAs(label);

                    TraversalHelper.insertStep(identityStep, 0, this);
                    TraversalHelper.insertStep(new HasStep(this, new HasContainer(Element.ID, Compare.EQUAL, element.id())), 0, this);
                    TraversalHelper.insertStep(new GiraphGraphStep<>(this, Edge.class, graph), 0, this);
                }
                return super.submit(engine);
            }
        };
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    public TinkerEdge getTinkerEdge() {
        return (TinkerEdge) this.element;
    }
}