package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMessenger<M extends Serializable> implements Messenger<M> {

    // Map<VertexId, MessageQueue>
    public Map<Object, Queue<M>> sendMessages = new HashMap<>();
    public Map<Object, Queue<M>> receiveMessages = new HashMap<>();

    public Iterable<M> receiveMessages(final Vertex vertex, final MessageType messageType) {
        if (messageType instanceof MessageType.Local) {
            final MessageType.Local<Object, M> localMessageType = (MessageType.Local) messageType;
            final Edge[] edge = new Edge[1]; // simulates storage side-effects available in Gremlin, but not Java8 streams
            return StreamFactory.iterable(StreamFactory.stream(localMessageType.getQuery().build().reverse().build(vertex))
                    .map(e -> {
                        edge[0] = e;
                        return receiveMessages.get(e.getVertex(localMessageType.getQuery().direction).getId());
                    })
                    .filter(q -> null != q)
                    .flatMap(q -> q.stream())
                    .map(message -> localMessageType.getEdgeFunction().apply(message, edge[0])));

        } else {
            return StreamFactory.iterable(Arrays.asList(vertex).stream()
                    .map(v -> this.receiveMessages.get(v.getId()))
                    .filter(q -> null != q)
                    .flatMap(q -> q.stream()));
        }
    }

    public void sendMessage(final Vertex vertex, final MessageType messageType, final M message) {
        if (messageType instanceof MessageType.Local) {
            getMessageList(vertex.getId()).add(message);
        } else {
            ((MessageType.Global) messageType).vertices().forEach(v -> {
                getMessageList(v.getId()).add(message);
            });
        }
    }

    private Queue<M> getMessageList(final Object vertexId) {
        Queue<M> messages = this.sendMessages.get(vertexId);
        if (null == messages) {
            messages = new ConcurrentLinkedQueue<>();
            this.sendMessages.put(vertexId, messages);
        }
        return messages;
    }

    public void completeIteration() {
        this.receiveMessages = this.sendMessages;
        this.sendMessages = new HashMap<>();
    }
}
