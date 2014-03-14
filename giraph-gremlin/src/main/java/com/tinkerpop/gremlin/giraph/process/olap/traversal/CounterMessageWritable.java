package com.tinkerpop.gremlin.giraph.process.olap.traversal;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalCounterMessage;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CounterMessageWritable implements Writable {

    private TraversalCounterMessage gremlinMessage;

    public void readFields(final DataInput input) {
        try {

            final byte[] objectBytes = new byte[input.readInt()];
            for (int i = 0; i < objectBytes.length; i++) {
                objectBytes[i] = input.readByte();
            }
            Kryo kryo = new Kryo();
            final ByteArrayInputStream inputStream = new ByteArrayInputStream(objectBytes);
            Input o = new Input(inputStream);
            this.gremlinMessage = kryo.readObject(o, TraversalCounterMessage.class);
            inputStream.close();
           /* final int objectLength = input.readInt();
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
            this.gremlinMessage.setCounter(input.readLong());*/
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    public void write(final DataOutput output) {
        try {

            Kryo kryo = new Kryo();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Output o = new Output(outputStream);
            kryo.writeObject(o, this.gremlinMessage);
            o.flush();
            output.writeInt(outputStream.size());
            output.write(outputStream.toByteArray());
            outputStream.close();
            /*final Holder holder = this.gremlinMessage.getHolder();
            final long counter = this.gremlinMessage.getCounter();
            final byte[] object = Serializer.serializeObject(holder.get());
            final int loops = holder.getLoops();
            final String future = holder.getFuture();

            output.writeInt(object.length);
            output.write(object);
            output.writeInt(loops);
            output.writeUTF(future);
            output.writeLong(counter);*/
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
