package com.tinkerpop.gremlin.giraph.process.computer;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.hadoop.io.LongWritable;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphMessenger implements Messenger<Serializable> {

    private final GiraphVertex giraphVertex;
    private final Iterable<KryoWritable> messages;

    public GiraphMessenger(final GiraphVertex giraphVertex, final Iterable<KryoWritable> messages) {
        this.giraphVertex = giraphVertex;
        this.messages = messages;
    }

    public Iterable<Serializable> receiveMessages(final Vertex vertex, final MessageType messageType) {
        return (Iterable) StreamFactory.iterable(StreamFactory.stream(this.messages).map(m -> m.get()));
    }

    public void sendMessage(final Vertex vertex, final MessageType messageType, final Serializable message) {
        if (messageType instanceof MessageType.Local) {
            final MessageType.Local<Object, Double> localMessageType = (MessageType.Local) messageType;
            localMessageType.vertices(vertex).forEach(v ->
                    this.giraphVertex.sendMessage(new LongWritable(Long.valueOf(v.id().toString())), new KryoWritable(message)));
        } else {
            final MessageType.Global globalMessageType = (MessageType.Global) messageType;
            globalMessageType.vertices().forEach(v ->
                    this.giraphVertex.sendMessage(new LongWritable(Long.valueOf(v.id().toString())), new KryoWritable(message)));
        }
    }
}
