package com.tinkerpop.blueprints.mailbox;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.Query;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Mailbox<M extends Serializable> {

    public Iterable<M> getMessages(Query query);

    public void sendMessage(Vertex vertex, Query query, M message);

}
