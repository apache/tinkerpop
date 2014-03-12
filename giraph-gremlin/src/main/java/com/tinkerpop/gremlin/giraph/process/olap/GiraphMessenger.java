package com.tinkerpop.gremlin.giraph.process.olap;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphMessenger implements Messenger<Double> {

    private final GiraphVertex giraphVertex;
    private final Iterable<DoubleWritable> messages;

    public GiraphMessenger(final GiraphVertex giraphVertex, final Iterable<DoubleWritable> messages) {
        this.giraphVertex = giraphVertex;
        this.messages = messages;
    }

    public Iterable<Double> receiveMessages(final Vertex vertex, final MessageType messageType) {
        return StreamFactory.iterable(StreamFactory.stream(this.messages).map(d -> d.get()));
    }

    public void sendMessage(final Vertex vertex, final MessageType messageType, final Double message) {
        this.giraphVertex.sendMessage(new LongWritable(new Long(vertex.getId().toString())), new DoubleWritable(message));
    }
}
