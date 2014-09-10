package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.computer.util.GiraphComputerHelper;
import com.tinkerpop.gremlin.giraph.process.graph.step.sideEffect.GiraphGraphStep;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.step.util.IdentityStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphEdge extends GiraphElement implements Edge, Serializable, WrappedEdge<TinkerEdge> {

    protected GiraphEdge() {
    }

    public GiraphEdge(final TinkerEdge edge, final GiraphGraph graph) {
        super(edge, graph);
    }

    @Override
    public GraphTraversal<Edge, Edge> start() {
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<Vertex, Vertex>(this.graph) {
            @Override
            public GraphTraversal<Vertex, Vertex> submit(final GraphComputer computer) {
                GiraphComputerHelper.prepareTraversalForComputer(this);
                final String label = this.getSteps().get(0).getLabel();
                TraversalHelper.removeStep(0, this);
                final Step identityStep = new IdentityStep(this);
                if (TraversalHelper.isLabeled(label))
                    identityStep.setLabel(label);

                TraversalHelper.insertStep(identityStep, 0, this);
                TraversalHelper.insertStep(new HasStep(this, new HasContainer(Element.ID, Compare.EQUAL, tinkerElement.id())), 0, this);
                TraversalHelper.insertStep(new GiraphGraphStep<>(this, Edge.class, graph), 0, this);

                return super.submit(computer);
            }
        };
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    @Override
    public TinkerEdge getBaseEdge() {
        return (TinkerEdge) this.tinkerElement;
    }

    @Override
    public Edge.Iterators iterators() {
        return this.iterators;
    }

    private final Edge.Iterators iterators = new Iterators();

    protected class Iterators implements Edge.Iterators, Serializable {

        @Override
        public Iterator<Vertex> vertices(final Direction direction) {
            final List<Vertex> vertices = new ArrayList<>();
            if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
                vertices.add(graph.v(getBaseEdge().iterators().vertices(Direction.OUT).next().id()));
            if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
                vertices.add(graph.v(getBaseEdge().iterators().vertices(Direction.IN).next().id()));
            return vertices.iterator();
        }

        @Override
        public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
            return (Iterator) StreamFactory.stream(getBaseEdge().iterators().properties(propertyKeys))
                    .map(property -> new GiraphProperty<>((TinkerProperty<V>) property, GiraphEdge.this)).iterator();
        }

        @Override
        public <V> Iterator<Property<V>> hiddens(final String... propertyKeys) {
            return (Iterator) StreamFactory.stream(getBaseEdge().iterators().hiddens(propertyKeys))
                    .map(property -> new GiraphProperty<>((TinkerProperty<V>) property, GiraphEdge.this)).iterator();
        }
    }
}