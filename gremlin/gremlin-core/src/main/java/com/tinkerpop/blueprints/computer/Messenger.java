package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Vertex;

import java.io.Serializable;

/**
 * The {@link Messenger} serves as the routing system for messages between vertices. For distributed systems,
 * the messenger can implement a "message passing" engine (distributed memory). For single machine systems, the
 * messenger can implement a "state sharing" engine (shared memory).
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Messenger<M extends Serializable> {

    public Iterable<M> receiveMessages(final Vertex vertex, final MessageType messageType);

    public void sendMessage(final Vertex vertex, final MessageType messageType, final M message);

}
