package com.tinkerpop.gremlin.driver.ser;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import groovy.json.JsonBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractJsonMessageSerializerV1d0 implements MessageSerializer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractJsonMessageSerializerV1d0.class);

    protected final TypeReference<Map<String, Object>> mapTypeReference = new TypeReference<Map<String, Object>>() {
    };

    abstract ObjectMapper obtainMapper();

    abstract byte[] obtainHeader();

    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final Map<String, Object> result = new HashMap<>();
            result.put(SerTokens.TOKEN_DATA, responseMessage.getResult().getData());
            result.put(SerTokens.TOKEN_META, responseMessage.getResult().getMeta());

            final Map<String, Object> status = new HashMap<>();
            status.put(SerTokens.TOKEN_MESSAGE, responseMessage.getStatus().getMessage());
            status.put(SerTokens.TOKEN_CODE, responseMessage.getStatus().getCode().getValue());
            status.put(SerTokens.TOKEN_ATTRIBUTES, responseMessage.getStatus().getAttributes());

            final Map<String, Object> message = new HashMap<>();
            message.put(SerTokens.TOKEN_STATUS, status);
            message.put(SerTokens.TOKEN_RESULT, result);
            message.put(SerTokens.TOKEN_REQUEST, responseMessage.getRequestId() != null ? responseMessage.getRequestId() : null);

            final byte[] payload = obtainMapper().writeValueAsBytes(message);
            encodedMessage = allocator.buffer(payload.length);
            encodedMessage.writeBytes(payload);

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn("Response [{}] could not be serialized by {}.", responseMessage.toString(), AbstractJsonMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public ByteBuf serializeRequestAsBinary(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final byte[] header = obtainHeader();
            final byte[] payload = obtainMapper().writeValueAsBytes(requestMessage);

            encodedMessage = allocator.buffer(header.length + payload.length);
            encodedMessage.writeBytes(header);
            encodedMessage.writeBytes(payload);

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn("Request [{}] could not be serialized by {}.", requestMessage.toString(), AbstractJsonMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public RequestMessage deserializeRequest(final ByteBuf msg) throws SerializationException {
        try {
            final byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);
            return obtainMapper().readValue(payload, RequestMessage.class);
        } catch (Exception ex) {
            logger.warn("Request [{}] could not be deserialized by {}.", msg, AbstractJsonMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public ResponseMessage deserializeResponse(final ByteBuf msg) throws SerializationException {
        try {
            final byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);
            final Map<String, Object> responseData = obtainMapper().readValue(payload, mapTypeReference);
            final Map<String, Object> status = (Map<String, Object>) responseData.get(SerTokens.TOKEN_STATUS);
            final Map<String, Object> result = (Map<String, Object>) responseData.get(SerTokens.TOKEN_RESULT);
            return ResponseMessage.build(UUID.fromString(responseData.get(SerTokens.TOKEN_REQUEST).toString()))
                    .code(ResponseStatusCode.getFromValue((Integer) status.get(SerTokens.TOKEN_CODE)))
                    .statusMessage(status.get(SerTokens.TOKEN_MESSAGE).toString())
                    .statusAttributes((Map<String, Object>) status.get(SerTokens.TOKEN_ATTRIBUTES))
                    .result(result.get(SerTokens.TOKEN_DATA))
                    .responseMetaData((Map<String, Object>) result.get(SerTokens.TOKEN_META))
                    .create();
        } catch (Exception ex) {
            logger.warn("Response [{}] could not be deserialized by {}.", msg, AbstractJsonMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    public static class GremlinServerModule extends SimpleModule {
        public GremlinServerModule() {
            super("graphson-gremlin-server");
            addSerializer(JsonBuilder.class, new JsonBuilderJacksonSerializer());
        }
    }

    public static class JsonBuilderJacksonSerializer extends StdSerializer<JsonBuilder> {
        public JsonBuilderJacksonSerializer() {
            super(JsonBuilder.class);
        }

        @Override
        public void serialize(final JsonBuilder json, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            // the JSON from the builder will already be started/ended as array or object...just need to surround it
            // with appropriate chars to fit into the serialization pattern.
            jsonGenerator.writeRaw(":");
            jsonGenerator.writeRaw(json.toString());
            jsonGenerator.writeRaw(",");
        }
    }
}
