package org.apache.tinkerpop.gremlin.driver.ser.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseResult;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatus;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;

import java.util.Map;
import java.util.UUID;

public class ResponseMessageSerializer implements TypeSerializer<ResponseMessage> {
    @Override
    public ResponseMessage read(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        // There is no type code / information for the response message itself.
        throw new SerializationException("ResponseMessageSerializer must not invoked with type information");
    }

    @Override
    public ResponseMessage readValue(ByteBuf buffer, GraphBinaryReader context, boolean nullable) throws SerializationException {
        final int version = buffer.readByte();
        assert version >>> 31 == 1;

        return ResponseMessage.build(context.readValue(buffer, UUID.class, true))
                .code(ResponseStatusCode.getFromValue(context.readValue(buffer, Integer.class, false)))
                .statusMessage(context.readValue(buffer, String.class, true))
                .statusAttributes(context.readValue(buffer, Map.class, false))
                .responseMetaData(context.readValue(buffer, Map.class, false))
                .result(context.read(buffer))
                .create();
    }

    @Override
    public ByteBuf write(ResponseMessage value, ByteBufAllocator allocator, GraphBinaryWriter context) throws SerializationException {
        // There is no type code / information for the response message itself.
        throw new SerializationException("ResponseMessageSerializer can not be written with type information");
    }

    @Override
    public ByteBuf writeValue(ResponseMessage value, ByteBufAllocator allocator, GraphBinaryWriter context, boolean nullable) throws SerializationException {
        final ResponseResult result = value.getResult();
        final ResponseStatus status = value.getStatus();

        return allocator.compositeBuffer(8).addComponents(true,
                // Version
                allocator.buffer(1).writeByte(0x81),
                // Nullable request id
                context.writeValue(value.getRequestId(), allocator, true),
                // Status code
                context.writeValue(status.getCode().getValue(), allocator, false),
                // Nullable status message
                context.writeValue(status.getMessage(), allocator, true),
                // Status attributes
                context.writeValue(status.getAttributes(), allocator, false),
                // Result meta
                context.writeValue(result.getMeta(), allocator, false),
                // Fully-qualified value
                context.write(result.getData(), allocator));
    }
}
