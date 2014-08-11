package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.giraph.Constants;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class KryoWritable<T> implements WritableComparable<KryoWritable> {

    T t;

    public KryoWritable() {
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

    public void readFields(final DataInput input) throws IOException {
        final int objectLength = WritableUtils.readVInt(input);
        final byte[] objectBytes = new byte[objectLength];
        for (int i = 0; i < objectLength; i++) {
            objectBytes[i] = input.readByte();
        }
        final Input in = new Input(new ByteArrayInputStream(objectBytes));
        this.t = (T) Constants.KRYO.readClassAndObject(in);
        in.close();
    }

    public void write(final DataOutput output) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Output out = new Output(outputStream);
        Constants.KRYO.writeClassAndObject(out, this.t);
        out.flush();
        WritableUtils.writeVInt(output, outputStream.toByteArray().length);
        output.write(outputStream.toByteArray());
        out.close();
    }

    public int compareTo(final KryoWritable kryoWritable) {
        return this.t instanceof Comparable ? ((Comparable) this.t).compareTo(kryoWritable.get()) : 1;
    }
}
