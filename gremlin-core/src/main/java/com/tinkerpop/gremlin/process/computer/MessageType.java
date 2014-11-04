package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * A {@link MessageType} represents the "address" of a message. A message can have multiple receivers and message type
 * allows the underlying {@link GraphComputer} to apply the message passing algorithm in whichever manner is most efficient.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class MessageType {

    /**
     * A Global message is directed at an arbitrary vertex in the graph.
     * The recipient vertex need not be adjacent to the sending vertex.
     * This message type should be avoided if a {@link Local} can be used.
     */
    public final static class Global extends MessageType {

        private final Iterable<Vertex> vertices;

        private Global(final Iterable<Vertex> vertices) {
            this.vertices = vertices;
        }

        public static Global of(final Iterable<Vertex> vertices) {
            return new Global(vertices);
        }

        public static Global of(final Vertex... vertices) {
            return new Global(Arrays.asList(vertices));
        }

        public Iterable<Vertex> vertices() {
            return this.vertices;
        }
    }

    /**
     * A Local message is directed to an adjacent (or "memory adjacent") vertex.
     * The adjacent vertex set is defined by the provided {@link Traversal} that dictates how to go from the sending vertex to the receiving vertex.
     * This is the preferred message type as it can potentially be optimized by the underlying {@link Messenger} implementation.
     * The preferred optimization is to not distribute a message object to all adjacent vertices.
     * Instead, allow the recipients to read a single message object stored at the "sending" vertex.
     * This is possible via `Traversal.reverse()`. This optimizations greatly reduces the amount of data created in the computation.
     *
     * @param <M1> The message type
     * @param <M2> The message type once modulated by the incident/propagating edge.
     */
    public final static class Local<M1, M2> extends MessageType {
        public final Supplier<? extends Traversal<Vertex, Edge>> incidentTraversal;
        public final BiFunction<M1, Edge, M2> edgeFunction;

        private Local(final Supplier<? extends Traversal<Vertex, Edge>> incidentTraversal) {
            this.incidentTraversal = incidentTraversal;
            this.edgeFunction = (final M1 m, final Edge e) -> (M2) m; // the default is an identity function
        }

        private Local(final Supplier<? extends Traversal<Vertex, Edge>> incidentTraversal, final BiFunction<M1, Edge, M2> edgeFunction) {
            this.incidentTraversal = incidentTraversal;
            this.edgeFunction = edgeFunction;
        }

        public static Local of(final Supplier<? extends Traversal<Vertex, Edge>> incidentTraversal) {
            return new Local(incidentTraversal);
        }

        public static <M1, M2> Local of(final Supplier<? extends Traversal<Vertex, Edge>> incidentTraversal, final BiFunction<M1, Edge, M2> edgeFunction) {
            return new Local<>(incidentTraversal, edgeFunction);
        }

        public <T extends Traversal<Vertex, Edge>> T edges(final Vertex vertex) {
            final Traversal<Vertex, Edge> traversal = this.incidentTraversal.get();
            TraversalHelper.insertStep(new StartStep<>(traversal, vertex), 0, traversal);
            return (T) traversal;
        }

        public <T extends Traversal<Vertex, Vertex>> T vertices(final Vertex vertex) {
            final Traversal traversal = this.incidentTraversal.get();
            final VertexStep step = TraversalHelper.getLastStep(traversal, VertexStep.class).get();
            TraversalHelper.insertStep(new EdgeVertexStep(traversal, step.getDirection().opposite()), traversal.getSteps().size(), traversal);
            TraversalHelper.insertStep(new StartStep<>(traversal, vertex), 0, traversal);
            return (T) traversal;
        }

        public Direction getDirection() {
            final Traversal traversal = this.incidentTraversal.get();
            final VertexStep step = TraversalHelper.getLastStep(traversal, VertexStep.class).get();
            return step.getDirection();
        }

        public BiFunction<M1, Edge, M2> getEdgeFunction() {
            return this.edgeFunction;
        }

        public Supplier<? extends Traversal<Vertex, Edge>> getIncidentTraversal() {
            return this.incidentTraversal;
        }
    }
}
