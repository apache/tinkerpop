package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.QueryBuilder;

import java.io.Serializable;

/**
 * The Messenger serves as the routing system for messages between vertices.
 * For distributed systems, the messenger can implement a "message passing" engine (distributed memory).
 * For single machine systems, the messenger can implement a "state sharing" engine (shared memory).
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Messenger<M extends Serializable> {

    public Iterable<M> receiveMessages(Vertex vertex, QueryBuilder query);

    public void sendMessage(Vertex vertex, QueryBuilder query, M message);

}
