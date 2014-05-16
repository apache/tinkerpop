package com.tinkerpop.gremlin.giraph.process.olap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.process.PathTraverser;
import com.tinkerpop.gremlin.process.SimpleTraverser;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class KryoWritable<T> implements Writable {

    public final Kryo KRYO = new Kryo();

    T t;

    public KryoWritable() {
        KRYO.register(SimpleTraverser.class);
        KRYO.register(PathTraverser.class);
        // TODO: We may need to create concrete ID numbers in cross JVM situations.
    }

    public KryoWritable(final T t) {
        this();
        this.t = t;
    }

    public T get() {
        return this.t;
    }

    public void readFields(final DataInput input) {
        try {
            final int objectLength = input.readInt();
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
            output.writeInt(outputStream.toByteArray().length);
            output.write(outputStream.toByteArray());
            out.close();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
