package com.tinkerpop.gremlin.giraph.process.olap.util;

import com.tinkerpop.gremlin.util.Serializer;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RuleWritable implements Writable {

    public enum Rule {
        OR, AND, INCR, SET, SET_IF_ABSENT
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

    public void readFields(final DataInput input) throws IOException {
        int length = input.readInt();
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = input.readByte();
        }
        try {
            this.object = Serializer.deserializeObject(bytes);
            this.rule = Rule.values()[input.readInt()];
        } catch (final ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    public void write(final DataOutput output) throws IOException {
        final byte[] bytes = Serializer.serializeObject(this.object);
        output.writeInt(bytes.length);
        output.write(bytes);
        output.writeInt(this.rule.ordinal());
    }

}
