package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.tinkerpop.gremlin.util.Serializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RuleWritable implements Writable {

    public enum Rule {
        OR, AND, INCR, SET, NO_OP
    }

    private Rule rule;
    private Object object;

    public RuleWritable() {

    }

    public RuleWritable(final Rule rule, final Object object) {
        this.rule = rule;
        this.object = object;
    }

    public <T> T getObject() {
        return (T) this.object;
    }

    public Rule getRule() {
        return this.rule;
    }

    // TODO: Don't use Kryo (its sin)

    @Override
    public void readFields(final DataInput input) throws IOException {
        this.rule = Rule.values()[WritableUtils.readVInt(input)];
        final int objectLength = WritableUtils.readVInt(input);
        final byte[] objectBytes = new byte[objectLength];
        for (int i = 0; i < objectLength; i++) {
            objectBytes[i] = input.readByte();
        }
        try {
            this.object = Serializer.deserializeObject(objectBytes);
        } catch (final ClassNotFoundException e) {
            throw new IOException(e.getMessage(), e);
        }

        /*this.rule = Rule.values()[WritableUtils.readVInt(input)];
        int objectLength = WritableUtils.readVInt(input);
        byte[] bytes = new byte[objectLength];
        for (int i = 0; i < objectLength; i++) {
            bytes[i] = input.readByte();
        }
        final Input in = new Input(new ByteArrayInputStream(bytes));
        this.object = Constants.KRYO.readClassAndObject(in);
        in.close();*/
    }

    @Override
    public void write(final DataOutput output) throws IOException {
        WritableUtils.writeVInt(output, this.rule.ordinal());
        final byte[] objectBytes = Serializer.serializeObject(this.object);
        WritableUtils.writeVInt(output, objectBytes.length);
        output.write(objectBytes);

        /*
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Output out = new Output(outputStream);
        Constants.KRYO.writeClassAndObject(out, this.object);
        out.flush();
        WritableUtils.writeVInt(output, this.rule.ordinal());
        WritableUtils.writeVInt(output, outputStream.toByteArray().length);
        output.write(outputStream.toByteArray());
        out.close(); */
    }

    public String toString() {
        return this.rule + ":" + this.object;
    }

}
