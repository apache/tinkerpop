package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.mailbox.Mailbox;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.io.Serializable;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMailbox<M extends Serializable> implements Mailbox<M> {

    public static final String MAILBOX = Property.Key.hidden("mailbox");
    //public static final String QUEUE = Property.Key.hidden("queue");

    //private final TinkerGraph graph;

    /*public TinkerMailbox(final TinkerGraph graph) {
        this.graph = graph;
    }*/

    public Iterable<M> getMessages(final Vertex vertex, final VertexQueryBuilder query) {
        return StreamFactory.stream(query.build(vertex).vertices()).map(v -> v.<M>getValue(MAILBOX)).collect(Collectors.<M>toList());
    }

    public void sendMessage(final Vertex vertex, final VertexQueryBuilder query, final M message) {
        vertex.setProperty(MAILBOX, message);
    }
}
