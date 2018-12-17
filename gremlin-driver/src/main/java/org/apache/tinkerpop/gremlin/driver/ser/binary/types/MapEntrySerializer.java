package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;

import java.util.HashMap;
import java.util.Map;

public class MapEntrySerializer extends SimpleTypeSerializer<Map.Entry> implements TransformSerializer<Map.Entry> {
    public MapEntrySerializer() {
        super(null);
    }

    @Override
    Map.Entry readValue(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        throw new SerializationException("A map entry should not be read individually");
    }

    @Override
    public ByteBuf writeValue(Map.Entry value, ByteBufAllocator allocator, GraphBinaryWriter context) throws SerializationException {
        throw new SerializationException("A map entry should not be written individually");
    }

    @Override
    public Object transform(Map.Entry value) {
        final Map map = new HashMap();
        map.put(value.getKey(), value.getValue());
        return map;
    }
}
