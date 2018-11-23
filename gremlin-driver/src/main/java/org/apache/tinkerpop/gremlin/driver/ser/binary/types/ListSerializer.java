package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;

import java.util.ArrayList;
import java.util.List;

public class ListSerializer extends SimpleTypeSerializer<List> {
    @Override
    public List readValue(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        final int length = buffer.readInt();

        ArrayList result = new ArrayList(length);
        for (int i = 0; i < length; i++) {
            result.add(context.read(buffer));
        }

        return result;
    }

    @Override
    DataType getDataType() {
        return DataType.LIST;
    }

    @Override
    public ByteBuf writeValueSequence(List value, ByteBufAllocator allocator, GraphBinaryWriter context) throws SerializationException {
        CompositeByteBuf result = allocator.compositeBuffer(1 + value.size());
        result.addComponent(true, allocator.buffer(4).writeInt(value.size()));

        for (Object item : value) {
            result.addComponent(
                    true,
                    context.write(item, allocator));
        }

        return result;
    }
}
