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
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.driver.message.ResultType;
import groovy.json.JsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractJsonMessageSerializerV1d0 implements MessageSerializer {
    private static final Logger logger = LoggerFactory.getLogger(JsonMessageSerializerV1d0.class);

    // todo: move tokens
    public static final String TOKEN_RESULT = "result";
    public static final String TOKEN_CODE = "code";
    public static final String TOKEN_CONTENT_TYPE = "contentType";
    public static final String TOKEN_REQUEST = "requestId";
    public static final String TOKEN_TYPE = "type";

    protected final TypeReference<Map<String,Object>> mapTypeReference = new TypeReference<Map<String,Object>>(){};

    abstract ObjectMapper obtainMapper();

    @Override
    public String serializeResponse(final ResponseMessage responseMessage) {
        try {
            final Map<String, Object> result = new HashMap<>();
            result.put(TOKEN_CODE, responseMessage.getCode().getValue());
            result.put(TOKEN_RESULT, responseMessage.getResult());
            result.put(TOKEN_REQUEST, responseMessage.getRequestId() != null ? responseMessage.getRequestId() : null);
            result.put(TOKEN_TYPE, responseMessage.getResultType().getValue());

            return obtainMapper().writeValueAsString(result);
        } catch (Exception ex) {
            logger.warn("Response [{}] could not be serialized by {}.", responseMessage.toString(), JsonMessageSerializerV1d0.class.getName());
            throw new RuntimeException("Error during serialization.", ex);
        }
    }

    @Override
    public String serializeRequest(final RequestMessage requestMessage) {
        try {
            return obtainMapper().writeValueAsString(requestMessage);
        } catch (Exception ex) {
            logger.warn("Request [{}] could not be serialized by {}.", requestMessage.toString(), JsonMessageSerializerV1d0.class.getName());
            throw new RuntimeException("Error during serialization.", ex);
        }
    }

    @Override
    public Optional<ResponseMessage> deserializeResponse(final String msg) {
        try {
            final Map<String,Object> responseData = obtainMapper().readValue(msg, mapTypeReference);
            // todo: content types in response?   this is a mess in terms of deserialization ....................
            return Optional.of(ResponseMessage.create(UUID.fromString(responseData.get(TOKEN_REQUEST).toString()), "")
                    .code(ResultCode.getFromValue((Integer) responseData.get(TOKEN_CODE)))
                    .result(responseData.get(TOKEN_RESULT))
                    .contents(ResultType.getFromValue((Integer) responseData.get(TOKEN_TYPE)))
                    .build());
        } catch (Exception ex) {
            logger.warn("Response [{}] could not be deserialized by {}.", msg, JsonMessageSerializerV1d0.class.getName());
            return Optional.empty();
        }
    }

    @Override
    public Optional<RequestMessage> deserializeRequest(final String msg) {
        try {
            return Optional.of(obtainMapper().readValue(msg, RequestMessage.class));
        } catch (Exception ex) {
            logger.warn("Request [{}] could not be deserialized by {}.", msg, JsonMessageSerializerV1d0.class.getName());
            return Optional.empty();
        }
    }

    public static class GremlinServerModule extends SimpleModule {
        public GremlinServerModule() {
            super("graphson-gremlin-server");

            // todo: avoid this dependency ?
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
