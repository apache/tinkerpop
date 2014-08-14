package com.tinkerpop.gremlin.giraph.process.computer.util;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.giraph.Constants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RuleWritable implements Writable {

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

    public void readFields(final DataInput input) throws IOException {
        this.rule = Rule.values()[WritableUtils.readVInt(input)];
        int objectLength = WritableUtils.readVInt(input);
        byte[] bytes = new byte[objectLength];
        for (int i = 0; i < objectLength; i++) {
            bytes[i] = input.readByte();
        }
        final Input in = new Input(new ByteArrayInputStream(bytes));
        this.object = Constants.KRYO.readClassAndObject(in);
        in.close();
    }

    public void write(final DataOutput output) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Output out = new Output(outputStream);
        Constants.KRYO.writeClassAndObject(out, this.object);
        out.flush();
        WritableUtils.writeVInt(output, this.rule.ordinal());
        WritableUtils.writeVInt(output, outputStream.toByteArray().length);
        output.write(outputStream.toByteArray());
        out.close();
    }

}
