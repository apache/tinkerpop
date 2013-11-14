package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.blueprints.query.util.QueryBuilder;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
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

    public Map<Object, Map<Long, Queue<M>>> sendMessages = new HashMap<>();
    public Map<Object, Map<Long, Queue<M>>> receiveMessages = new HashMap<>();

    public Iterable<M> receiveMessages(final Vertex vertex, final QueryBuilder query) {
        if (query instanceof VertexQueryBuilder) {
            final long fingerPrint = query.fingerPrint();
            return StreamFactory.stream(((VertexQueryBuilder) query).build().reverse().build(vertex).vertices())
                    .map(v -> this.receiveMessages.get(v.getId()))
                    .filter(m -> null != m)
                    .map(m -> m.get(fingerPrint))
                    .filter(l -> null != l)
                    .flatMap(l -> l.stream())
                    .collect(Collectors.<M>toList());
        } else {
            return Arrays.asList(vertex).stream()
                    .map(v -> this.receiveMessages.get(v.getId()))
                    .filter(m -> null != m)
                    .map(m -> m.get(Long.MIN_VALUE))
                    .filter(l -> null != l)
                    .flatMap(l -> l.stream())
                    .collect(Collectors.<M>toList());
        }
    }

    public void sendMessage(final Vertex vertex, final QueryBuilder query, final M message) {
        if (query instanceof VertexQueryBuilder) {
            getMessageList(vertex.getId(), query.fingerPrint()).add(message);
        } else {
            query.vertices().forEach(v -> {
                getMessageList(v.getId(), Long.MIN_VALUE).add(message);
            });
        }
    }

    private Queue<M> getMessageList(final Object vertexId, final long fingerPrint) {
        Map<Long, Queue<M>> messages = this.sendMessages.get(vertexId);
        if (null == messages) {
            messages = new HashMap<>();
            this.sendMessages.put(vertexId, messages);
        }
        Queue<M> messageList = messages.get(fingerPrint);
        if (null == messageList) {
            messageList = new ConcurrentLinkedQueue<>();
            messages.put(fingerPrint, messageList);
        }
        return messageList;
    }

    public void completeIteration() {
        this.receiveMessages = this.sendMessages;
        this.sendMessages = new HashMap<>();
    }
}
