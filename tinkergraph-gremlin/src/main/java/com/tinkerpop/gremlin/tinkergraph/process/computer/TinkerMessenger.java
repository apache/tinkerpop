package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMessenger<M> implements Messenger<M> {

    private final Vertex vertex;
    private final TinkerMessageBoard<M> messageBoard;
    //private final Optional<MessageCombiner<M>> combiner;


    public TinkerMessenger(final Vertex vertex, final TinkerMessageBoard<M> messageBoard) {
        this.vertex = vertex;
        this.messageBoard = messageBoard;
        //this.combiner = combiner;
    }

    @Override
    public Iterable<M> receiveMessages(final MessageType messageType) {
        if (messageType instanceof MessageType.Local) {
            final MessageType.Local<Object, M> localMessageType = (MessageType.Local) messageType;
            final Edge[] edge = new Edge[1]; // simulates storage side-effects available in Gremlin, but not Java8 streams
            return StreamFactory.iterable(StreamFactory.stream(localMessageType.edges(this.vertex).reverse())
                    .map(e -> {
                        edge[0] = e;
                        return this.messageBoard.receiveMessages.get(e.toV(localMessageType.getDirection()).next());
                    })
                    .filter(q -> null != q)
                    .flatMap(q -> q.stream())
                    .map(message -> localMessageType.getEdgeFunction().apply(message, edge[0])));

        } else {
            return StreamFactory.iterable(Arrays.asList(this.vertex).stream()
                    .map(this.messageBoard.receiveMessages::get)
                    .filter(q -> null != q)
                    .flatMap(q -> q.stream()));
        }
    }

    @Override
    public void sendMessage(final MessageType messageType, final M message) {
        if (messageType instanceof MessageType.Local) {
            getMessageList(this.vertex).add(message);
        } else {
            ((MessageType.Global) messageType).vertices().forEach(v -> {
                final Queue<M> queue = getMessageList(v);
                /*if (this.combiner.isPresent() && !queue.isEmpty()) {
                    this.combiner.get().combine(queue.remove(), message).forEachRemaining(queue::add);
                } else*/
                queue.add(message);
            });
        }
    }

    private Queue<M> getMessageList(final Vertex vertex) {
        Queue<M> messages = this.messageBoard.sendMessages.get(vertex);
        if (null == messages) {
            messages = new ConcurrentLinkedQueue<>();
            this.messageBoard.sendMessages.put(vertex, messages);
        }
        return messages;
    }
}
