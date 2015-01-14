package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MessageCombiner;
import com.tinkerpop.gremlin.process.computer.MessageScope;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMessenger<M> implements Messenger<M> {

    private final Vertex vertex;
    private final TinkerMessageBoard<M> messageBoard;
    private final MessageCombiner<M> combiner;


    public TinkerMessenger(final Vertex vertex, final TinkerMessageBoard<M> messageBoard, final Optional<MessageCombiner<M>> combiner) {
        this.vertex = vertex;
        this.messageBoard = messageBoard;
        this.combiner = combiner.isPresent() ? combiner.get() : null;
    }

    @Override
    public Iterable<M> receiveMessages(final MessageScope messageScope) {
        if (messageScope instanceof MessageScope.Local) {
            final MessageScope.Local<M> localMessageScope = (MessageScope.Local) messageScope;
            final Traversal<Vertex, Edge> incidentTraversal = TinkerMessenger.setVertexStart(localMessageScope.getIncidentTraversal().get(), this.vertex);
            final Direction direction = TinkerMessenger.getDirection(incidentTraversal);
            final Edge[] edge = new Edge[1]; // simulates storage side-effects available in Gremlin, but not Java8 streams
            return StreamFactory.iterable(StreamFactory.stream(incidentTraversal.asAdmin().reverse())
                    .map(e -> this.messageBoard.receiveMessages.get((edge[0] = e).iterators().vertexIterator(direction).next()))
                    .filter(q -> null != q)
                    .flatMap(q -> q.stream())
                    .map(message -> localMessageScope.getEdgeFunction().apply(message, edge[0])));

        } else {
            return StreamFactory.iterable(Stream.of(this.vertex)
                    .map(this.messageBoard.receiveMessages::get)
                    .filter(q -> null != q)
                    .flatMap(q -> q.stream()));
        }
    }

    @Override
    public void sendMessage(final MessageScope messageScope, final M message) {
        if (messageScope instanceof MessageScope.Local) {
            addMessage(this.vertex, message);
        } else {
            ((MessageScope.Global) messageScope).vertices().forEach(v -> addMessage(v, message));
        }
    }

    private final void addMessage(final Vertex vertex, final M message) {
        final Queue<M> queue = this.messageBoard.sendMessages.computeIfAbsent(vertex, v -> new ConcurrentLinkedQueue<>());
        synchronized (queue) {
            queue.add(null != this.combiner && !queue.isEmpty() ? this.combiner.combine(queue.remove(), message) : message);
        }
    }

    ///////////

    private static <T extends Traversal<Vertex, Edge>> T setVertexStart(final Traversal<Vertex, Edge> incidentTraversal, final Vertex vertex) {
        final Traversal<Vertex, Edge> traversal = incidentTraversal;
        traversal.asAdmin().addStep(0,new StartStep<>(traversal, vertex));
        return (T) traversal;
    }

    private static Direction getDirection(final Traversal<Vertex, Edge> incidentTraversal) {
        final VertexStep step = TraversalHelper.getLastStep(incidentTraversal, VertexStep.class).get();
        return step.getDirection();
    }
}
