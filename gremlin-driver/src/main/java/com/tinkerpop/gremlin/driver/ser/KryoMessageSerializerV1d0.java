package com.tinkerpop.gremlin.driver.ser;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.driver.message.ResultType;
import com.tinkerpop.gremlin.structure.io.kryo.GremlinKryo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class KryoMessageSerializerV1d0 implements MessageSerializer {
    // todo: maybe need a special interface for MessageSerializer because implementers can only binary serializers
    // todo: how will client and server side users of kryo configure custom classes
    private static final GremlinKryo gremlinKryo = GremlinKryo.create(GremlinKryo.Version.V_1_0_0).build();
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final String MIME_TYPE = Serializers.KRYO_V1D0.getValue();

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{MIME_TYPE};
    }

    @Override
    public ResponseMessage deserializeResponse(final ByteBuf msg) throws SerializationException{
        try {
            final Kryo kryo = gremlinKryo.createKryo();
            final byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);
            try (final Input input = new Input(payload)) {
                final Map<String,Object> responseData = (Map<String,Object>) kryo.readClassAndObject(input);
                return ResponseMessage.create(UUID.fromString(responseData.get(SerTokens.TOKEN_REQUEST).toString()))
                        .code(ResultCode.getFromValue((Integer) responseData.get(SerTokens.TOKEN_CODE)))
                        .result(responseData.get(SerTokens.TOKEN_RESULT))
                        .contents(ResultType.getFromValue((Integer) responseData.get(SerTokens.TOKEN_TYPE)))
                        .build();
            }
        } catch (Exception ex) {
            logger.warn("Response [{}] could not be deserialized by {}.", msg, KryoMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final Map<String, Object> result = new HashMap<>();
            result.put(SerTokens.TOKEN_CODE, responseMessage.getCode().getValue());
            result.put(SerTokens.TOKEN_RESULT, responseMessage.getResult());
            result.put(SerTokens.TOKEN_REQUEST, responseMessage.getRequestId() != null ? responseMessage.getRequestId() : null);
            result.put(SerTokens.TOKEN_TYPE, responseMessage.getResultType().getValue());

            final Kryo kryo = gremlinKryo.createKryo();
            try (final OutputStream baos = new ByteArrayOutputStream()) {
                final Output output = new Output(baos);
                kryo.writeClassAndObject(output, result);

                final long size = output.total();
                if (size > Integer.MAX_VALUE)
                    throw new SerializationException(String.format("Message size of %s exceeds allocatable space", size));

                encodedMessage = allocator.buffer((int) output.total());
                encodedMessage.writeBytes(output.toBytes());
            }

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn("Response [{}] could not be serialized by {}.", responseMessage.toString(), KryoMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public RequestMessage deserializeRequest(final ByteBuf msg) throws SerializationException {
        try {
            final Kryo kryo = gremlinKryo.createKryo();
            final byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);
            try (final Input input = new Input(payload)) {
                final Map<String,Object> requestData = (Map<String,Object>) kryo.readClassAndObject(input);
                final RequestMessage.Builder builder = RequestMessage.create((String) requestData.get(SerTokens.TOKEN_OP))
                        .overrideRequestId((UUID) requestData.get(SerTokens.TOKEN_REQUEST))
                        .processor((String) requestData.get(SerTokens.TOKEN_PROCESSOR));
                final Map<String,Object> args = (Map<String,Object>) requestData.get(SerTokens.TOKEN_ARGS);
                args.forEach(builder::addArg);
                return builder.build();
            }
        } catch (Exception ex) {
            logger.warn("Request [{}] could not be deserialized by {}.", msg, KryoMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public ByteBuf serializeRequestAsBinary(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final Kryo kryo = gremlinKryo.createKryo();
            try (final OutputStream baos = new ByteArrayOutputStream()) {
                final Output output = new Output(baos);
                output.writeByte(MIME_TYPE.length());
                output.write(MIME_TYPE.getBytes(UTF8));

                final Map<String, Object> request = new HashMap<>();
                request.put(SerTokens.TOKEN_REQUEST, requestMessage.getRequestId());
                request.put(SerTokens.TOKEN_PROCESSOR, requestMessage.getProcessor());
                request.put(SerTokens.TOKEN_OP, requestMessage.getOp());
                request.put(SerTokens.TOKEN_ARGS, requestMessage.getArgs());

                kryo.writeClassAndObject(output, request);

                final long size = output.total();
                if (size > Integer.MAX_VALUE)
                    throw new SerializationException(String.format("Message size of %s exceeds allocatable space", size));

                encodedMessage = allocator.buffer((int) output.total());
                encodedMessage.writeBytes(output.toBytes());
            }

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn("Request [{}] could not be serialized by {}.", requestMessage.toString(), KryoMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }
}
