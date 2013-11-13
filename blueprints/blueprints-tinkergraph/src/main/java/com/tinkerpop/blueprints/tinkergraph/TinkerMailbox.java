package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.Mailbox;
import com.tinkerpop.blueprints.query.util.QueryBuilder;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMailbox<M extends Serializable> implements Mailbox<M> {

    private static final String MAILBOX = Property.Key.hidden("mailbox");

    //private final TinkerGraph graph;

    /*public TinkerMailbox(final TinkerGraph graph) {
        this.graph = graph;
    }*/

    public Iterable<M> getMessages(final Vertex vertex, final QueryBuilder query) {
        if (query instanceof VertexQueryBuilder) {
            long fingerPrint = ((VertexQueryBuilder) query).build().reverse().fingerPrint();
            return StreamFactory.stream(((VertexQueryBuilder) query).build(vertex).vertices())
                    .map(v -> v.<Map<Long, M>>getValue(MAILBOX).get(fingerPrint))
                    .collect(Collectors.<M>toList());
        } else {
            // TODO implement what happens when you reference arbitrary vertices
            throw new UnsupportedOperationException();
        }
    }

    public void sendMessage(final Vertex vertex, final QueryBuilder query, final M message) {
        if (query instanceof VertexQueryBuilder) {
            if (!vertex.getProperty(MAILBOX).isPresent())
                vertex.setProperty(MAILBOX, new HashMap<>());

            Map<Long, M> messages = vertex.getValue(MAILBOX);
            messages.put(query.fingerPrint(), message);
        } else {
            // TODO implement what happens when you reference arbitrary vertices
            throw new UnsupportedOperationException();
        }
    }
}
