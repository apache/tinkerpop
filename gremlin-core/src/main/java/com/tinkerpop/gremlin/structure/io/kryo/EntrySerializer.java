package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.AbstractMap;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class EntrySerializer extends Serializer<Map.Entry> {
    @Override
    public void write(final Kryo kryo, final Output output, final Map.Entry entry) {
        kryo.writeClassAndObject(output, entry.getKey());
        kryo.writeClassAndObject(output, entry.getValue());
    }

    @Override
    public Map.Entry read(final Kryo kryo, final Input input, final Class<Map.Entry> entryClass) {
        return new AbstractMap.SimpleEntry(kryo.readClassAndObject(input), kryo.readClassAndObject(input));
    }
}
