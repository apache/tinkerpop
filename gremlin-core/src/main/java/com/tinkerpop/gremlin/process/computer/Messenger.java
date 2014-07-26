package com.tinkerpop.gremlin.process.computer;

import java.io.Serializable;

/**
 * The {@link Messenger} serves as the routing system for messages between vertices. For distributed systems,
 * the messenger can implement a "message passing" engine (distributed memory). For single machine systems, the
 * messenger can implement a "state sharing" engine (shared memory). Each messenger is tied to the particular
 * vertex distributing the message.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Messenger<M extends Serializable> {

    public Iterable<M> receiveMessages(final MessageType messageType);

    public void sendMessage(final MessageType messageType, final M message);

}
