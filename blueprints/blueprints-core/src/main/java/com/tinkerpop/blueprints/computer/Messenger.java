package com.tinkerpop.blueprints.computer;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * The Messenger serves as the routing system for messages between vertices.
 * For distributed systems, the messenger can implement a "message passing" engine (distributed memory).
 * For single machine systems, the messenger can implement a "state sharing" engine (shared memory).
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Messenger<M extends Serializable> {

    public Iterable<M> receiveMessages(final Vertex vertex, final MessageType messageType);

    public void sendMessage(final Vertex vertex, final MessageType messageType, final M message);

    public static List<Vertex> getHostingVertices(final Object object) {
        if (object instanceof Vertex)
            return Arrays.asList((Vertex) object);
        else if (object instanceof Edge)
            return Arrays.asList(((Edge) object).getVertex(Direction.OUT), ((Edge) object).getVertex(Direction.IN));
        else if (object instanceof Property)
            return getHostingVertices(((Property) object).getElement());
        else {
            throw new IllegalStateException("The host of the object is unknown: " + object.toString());
        }
    }

}
