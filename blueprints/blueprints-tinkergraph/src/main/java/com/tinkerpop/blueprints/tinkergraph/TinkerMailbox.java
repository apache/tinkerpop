package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.mailbox.Mailbox;
import com.tinkerpop.blueprints.query.util.QueryBuilder;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.io.Serializable;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMailbox<M extends Serializable> implements Mailbox<M> {

    public static final String MAILBOX = Property.Key.hidden("mailbox");

    //private final TinkerGraph graph;

    /*public TinkerMailbox(final TinkerGraph graph) {
        this.graph = graph;
    }*/

    public Iterable<M> getMessages(final Vertex vertex, final QueryBuilder query) {
        if (query instanceof VertexQueryBuilder) {
            return StreamFactory.stream(((VertexQueryBuilder) query).build(vertex).vertices()).map(v -> v.<M>getValue(MAILBOX)).collect(Collectors.<M>toList());
        } else {
            // TODO implement what happens when you reference arbitrary vertices
            throw new UnsupportedOperationException();
        }
    }

    public void sendMessage(final Vertex vertex, final QueryBuilder query, final M message) {
        if (query instanceof VertexQueryBuilder) {
            vertex.setProperty(MAILBOX, message);
        } else {
            // TODO implement what happens when you reference arbitrary vertices
            throw new UnsupportedOperationException();
        }
    }
}
