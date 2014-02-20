package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.process.olap.MessageType;
import com.tinkerpop.gremlin.process.olap.Messenger;
import com.tinkerpop.gremlin.structure.util.StreamFactory;

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

    // Map<VertexId, Map<MessageLabel, MessageQueue>>>
    public Map<Object, Map<String, Queue<M>>> sendMessages = new HashMap<>();
    public Map<Object, Map<String, Queue<M>>> receiveMessages = new HashMap<>();

    public Iterable<M> receiveMessages(final Vertex vertex, final MessageType messageType) {
        if (messageType instanceof MessageType.Local) {
            final MessageType.Local<Object, M> localMessageType = (MessageType.Local) messageType;
            final Edge[] edge = new Edge[1]; // simulates storage side-effects available in Gremlin, but not Java8 streams
            return StreamFactory.iterable(StreamFactory.stream(localMessageType.getQuery().build().reverse().build(vertex))
                    .map(e -> {
                        edge[0] = e;
                        return receiveMessages.get(e.getVertex(localMessageType.getQuery().direction).getId());
                    })
                    .filter(m -> null != m)
                    .map(m -> m.get(messageType.getLabel()))
                    .filter(l -> null != l)
                    .flatMap(l -> l.stream())
                    .map(message -> localMessageType.getEdgeFunction().apply(message, edge[0])));

        } else {
            return StreamFactory.iterable(Arrays.asList(vertex).stream()
                    .map(v -> this.receiveMessages.get(v.getId()))
                    .filter(m -> null != m)
                    .map(m -> m.get(messageType.getLabel()))
                    .filter(l -> null != l)
                    .flatMap(l -> l.stream()));
        }
    }

    public void sendMessage(final Vertex vertex, final MessageType messageType, final M message) {
        if (messageType instanceof MessageType.Local) {
            getMessageList(vertex.getId(), messageType).add(message);
        } else {
            ((MessageType.Global) messageType).vertices().forEach(v -> {
                getMessageList(v.getId(), messageType).add(message);
            });
        }
    }

    private Queue<M> getMessageList(final Object vertexId, final MessageType messageType) {
        Map<String, Queue<M>> messages = this.sendMessages.get(vertexId);
        if (null == messages) {
            messages = new HashMap<>();
            this.sendMessages.put(vertexId, messages);
        }
        Queue<M> messageList = messages.get(messageType.getLabel());
        if (null == messageList) {
            messageList = new ConcurrentLinkedQueue<>();
            messages.put(messageType.getLabel(), messageList);
        }
        return messageList;
    }

    public void completeIteration() {
        this.receiveMessages = this.sendMessages;
        this.sendMessages = new HashMap<>();
    }
}
