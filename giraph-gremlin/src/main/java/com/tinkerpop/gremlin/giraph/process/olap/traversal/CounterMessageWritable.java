package com.tinkerpop.gremlin.giraph.process.olap.traversal;

import com.tinkerpop.gremlin.giraph.structure.io.util.Serializer;
import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.SimpleHolder;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalCounterMessage;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CounterMessageWritable implements Writable {

    private TraversalCounterMessage gremlinMessage;

    public void readFields(final DataInput input) {
        try {
            final int objectLength = input.readInt();
            final byte[] objectBytes = new byte[objectLength];
            for (int i = 0; i < objectLength; i++) {
                objectBytes[i] = input.readByte();
            }
            final Object object = Serializer.deserializeObject(objectBytes);
            final SimpleHolder holder = new SimpleHolder(object);
            final int loops = input.readInt();
            for (int i = 0; i < loops; i++) {
                holder.incrLoops();
            }
            holder.setFuture(input.readUTF());
            this.gremlinMessage = TraversalCounterMessage.of(holder);
            this.gremlinMessage.setCounter(input.readLong());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    public void write(final DataOutput output) {
        try {
            final Holder holder = this.gremlinMessage.getHolder();
            final long counter = this.gremlinMessage.getCounter();
            final byte[] object = Serializer.serializeObject(holder.get());
            final int loops = holder.getLoops();
            final String future = holder.getFuture();

            output.writeInt(object.length);
            output.write(object);
            output.writeInt(loops);
            output.writeUTF(future);
            output.writeLong(counter);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
