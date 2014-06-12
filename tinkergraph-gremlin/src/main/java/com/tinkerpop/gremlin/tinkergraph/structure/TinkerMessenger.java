package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMessenger<M extends Serializable> implements Messenger<M> {

    private Vertex vertex;
    private TinkerMessageBoard<M> messageBoard;


    public TinkerMessenger(final Vertex vertex, final TinkerMessageBoard<M> messageBoard) {
        this.vertex = vertex;
        this.messageBoard = messageBoard;
    }

    public Iterable<M> receiveMessages(final MessageType messageType) {
        if (messageType instanceof MessageType.Local) {
            final MessageType.Local<Object, M> localMessageType = (MessageType.Local) messageType;
            final Edge[] edge = new Edge[1]; // simulates storage side-effects available in Gremlin, but not Java8 streams
            return StreamFactory.iterable(StreamFactory.stream(localMessageType.edges(vertex).reverse())
                    .map(e -> {
                        edge[0] = e;
                        return (localMessageType.getDirection().equals(Direction.OUT)) ?
                                this.messageBoard.receiveMessages.get(e.outV().id().next()) :
                                this.messageBoard.receiveMessages.get(e.inV().id().next());

                    })
                    .filter(q -> null != q)
                    .flatMap(q -> q.stream())
                    .map(message -> localMessageType.getEdgeFunction().apply(message, edge[0])));

        } else {
            return StreamFactory.iterable(Arrays.asList(vertex).stream()
                    .map(v -> this.messageBoard.receiveMessages.get(v.id()))
                    .filter(q -> null != q)
                    .flatMap(q -> q.stream()));
        }
    }

    public void sendMessage(final MessageType messageType, final M message) {
        if (messageType instanceof MessageType.Local) {
            getMessageList(vertex.id()).add(message);
        } else {
            ((MessageType.Global) messageType).vertices().forEach(v -> {
                getMessageList(v.id()).add(message);
            });
        }
    }

    private Queue<M> getMessageList(final Object vertexId) {
        Queue<M> messages = this.messageBoard.sendMessages.get(vertexId);
        if (null == messages) {
            messages = new ConcurrentLinkedQueue<>();
            this.messageBoard.sendMessages.put(vertexId, messages);
        }
        return messages;
    }
}
