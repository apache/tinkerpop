package com.tinkerpop.gremlin.giraph.process.computer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class KryoWritable<T> implements WritableComparable<KryoWritable> {

    private static final Kryo KRYO = new Kryo();

    T t;

    public KryoWritable() {
        //KRYO.register(SimpleTraverser.class);
        //KRYO.register(PathTraverser.class);
    }

    public KryoWritable(final T t) {
        this();
        this.t = t;
    }

    public T get() {
        return this.t;
    }

    public void set(final T t) {
        this.t = t;
    }

    @Override
    public String toString() {
        return this.t.toString();
    }

    public void readFields(final DataInput input) {
        try {
            final int objectLength = WritableUtils.readVInt(input);
            final byte[] objectBytes = new byte[objectLength];
            for (int i = 0; i < objectLength; i++) {
                objectBytes[i] = input.readByte();
            }
            this.t = (T) KRYO.readClassAndObject(new Input(new ByteArrayInputStream(objectBytes)));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    public void write(final DataOutput output) {
        try {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            final Output out = new Output(outputStream);
            KRYO.writeClassAndObject(out, this.t);
            out.flush();
            WritableUtils.writeVInt(output, outputStream.toByteArray().length);
            output.write(outputStream.toByteArray());
            out.close();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public int compareTo(final KryoWritable kryoWritable) {
        return this.t instanceof Comparable ? ((Comparable) this.t).compareTo(kryoWritable.get()) : 1;
    }
}
