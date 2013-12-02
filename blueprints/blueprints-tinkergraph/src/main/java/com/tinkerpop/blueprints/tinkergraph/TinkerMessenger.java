package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.MessageType;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMessenger<M extends Serializable> implements Messenger<M> {

    public Map<Object, Map<Class<? extends MessageType>, Queue<M>>> sendMessages = new HashMap<>();
    public Map<Object, Map<Class<? extends MessageType>, Queue<M>>> receiveMessages = new HashMap<>();

    public Iterable<M> receiveMessages(final Vertex vertex, final MessageType messageType) {
        if (messageType instanceof MessageType.Adjacent) {
            return StreamFactory.stream(((MessageType.Adjacent) messageType).reverse().vertices(vertex))
                    .map(v -> this.receiveMessages.get(v.getId()))
                    .filter(m -> null != m)
                    .map(m -> m.get(messageType.getClass()))
                    .filter(l -> null != l)
                    .flatMap(l -> l.stream())
                    .collect(Collectors.<M>toList());
        } else {
            return null;
            /*return Arrays.asList(vertex).stream()
                    .map(v -> this.receiveMessages.get(v.getId()))
                    .filter(m -> null != m)
                    .map(m -> m.get(messageType.getClass()))
                    .filter(l -> null != l)
                    .flatMap(l -> l.stream())
                    .collect(Collectors.<M>toList()); */
        }
    }

    public void sendMessage(final Vertex vertex, final MessageType messageType, final M message) {
        if (messageType instanceof MessageType.Adjacent) {
            getMessageList(vertex.getId(), messageType).add(message);
        } else {
          System.out.println("BABABA!");
           /* ((MessageType.Direct) messageType).vertices().forEach(v -> {
                getMessageList(v.getId(), messageType).add(message);
            });*/
        }
    }

    private Queue<M> getMessageList(final Object vertexId, final MessageType messageType) {
        Map<Class<? extends MessageType>, Queue<M>> messages = this.sendMessages.get(vertexId);
        if (null == messages) {
            messages = new HashMap<>();
            this.sendMessages.put(vertexId, messages);
        }
        Queue<M> messageList = messages.get(messageType.getClass());
        if (null == messageList) {
            messageList = new ConcurrentLinkedQueue<>();
            messages.put(messageType.getClass(), messageList);
        }
        return messageList;
    }

    public void completeIteration() {
        this.receiveMessages = this.sendMessages;
        this.sendMessages = new HashMap<>();
    }
}
