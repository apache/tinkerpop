package com.tinkerpop.blueprints.mailbox;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.VertexQueryBuilder;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Mailbox<M extends Serializable> {

    public Iterable<M> getMessages(Vertex vertex, VertexQueryBuilder query);

    public void sendMessage(Vertex vertex, VertexQueryBuilder query, M message);

}
