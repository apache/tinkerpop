package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.util.Serializer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GremlinWritable<T> implements WritableComparable<GremlinWritable> {

    T t;

    public GremlinWritable() {
    }

    public GremlinWritable(final T t) {
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

    @Override
    public void readFields(final DataInput input) throws IOException {
        try {
            this.t = (T) Serializer.deserializeObject(WritableUtils.readCompressedByteArray(input));
        } catch (final ClassNotFoundException e) {
            throw new IOException(e.getMessage(), e);
        }
        //this.t = (T) Constants.KRYO.readClassAndObject(new Input(new ByteArrayInputStream(WritableUtils.readCompressedByteArray(input))));
    }

    @Override
    public void write(final DataOutput output) throws IOException {
        WritableUtils.writeCompressedByteArray(output, Serializer.serializeObject(this.t));
        /*final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Output out = new Output(outputStream);
        Constants.KRYO.writeClassAndObject(out, this.t);
        out.flush();
        WritableUtils.writeCompressedByteArray(output, outputStream.toByteArray());
        out.close();*/
    }

    @Override
    public int compareTo(final GremlinWritable gremlinWritable) {
        return this.t instanceof Comparable ? ((Comparable) this.t).compareTo(gremlinWritable.get()) : 1;
    }
}
