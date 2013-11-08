package com.tinkerpop.blueprints.mailbox;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.QueryBuilder;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Mailbox<M extends Serializable> {

    public static final String MAILBOX = Property.Key.hidden("mailbox");

    public Iterable<M> getMessages(Vertex vertex, QueryBuilder query);

    public void sendMessage(Vertex vertex, QueryBuilder query, M message);

}
