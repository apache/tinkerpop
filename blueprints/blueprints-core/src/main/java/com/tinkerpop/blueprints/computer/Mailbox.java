package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.QueryBuilder;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Mailbox<M extends Serializable> {

    public Iterable<M> getMessages(Vertex vertex, QueryBuilder query);

    public void sendMessage(Vertex vertex, QueryBuilder query, M message);

}
