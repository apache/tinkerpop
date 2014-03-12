package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphMessenger<M extends Serializable> implements Messenger<M> {

    public Iterable<M> receiveMessages(final Vertex vertex, final MessageType messageType) {
        return null;
    }

    public void sendMessage(final Vertex vertex, final MessageType messageType, final M message) {
        // ((GiraphVertex) vertex).sendMessage(vertex.getId(), message);
    }
}
