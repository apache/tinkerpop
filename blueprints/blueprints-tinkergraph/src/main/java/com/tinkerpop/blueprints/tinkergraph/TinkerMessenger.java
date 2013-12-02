package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.MessageType;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMessenger<M extends Serializable> implements Messenger<M> {

    public Map<Object, Map<String, Queue<M>>> sendMessages = new HashMap<>();
    public Map<Object, Map<String, Queue<M>>> receiveMessages = new HashMap<>();

    public Iterable<M> receiveMessages(final Vertex vertex, final MessageType messageType) {
        if (messageType instanceof MessageType.Adjacent) {
            MessageType.Adjacent<Object, M> adjacentMessageType = (MessageType.Adjacent) messageType;

            final List<Edge> edge = new ArrayList<>(1); // simulates storage side-effects available in Gremlin, but not Java8 streams
            return StreamFactory.iterable(StreamFactory.stream(adjacentMessageType.getQuery().build().reverse().build(vertex).edges())
                    .map(e -> {
                        edge.clear();
                        edge.add(e);
                        return receiveMessages.get(e.getVertex(adjacentMessageType.getQuery().direction).getId());
                    })
                    .filter(m -> null != m)
                    .map(m -> m.get(messageType.getLabel()))
                    .filter(l -> null != l)
                    .flatMap(l -> l.stream())
                    .map(message -> adjacentMessageType.getFunction().apply(edge.get(0), message)));

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
        if (messageType instanceof MessageType.Adjacent) {
            getMessageList(vertex.getId(), messageType).add(message);
        } else {
            ((MessageType.Direct) messageType).vertices().forEach(v -> {
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
