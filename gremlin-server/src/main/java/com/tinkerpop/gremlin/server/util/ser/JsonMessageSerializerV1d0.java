package com.tinkerpop.gremlin.server.util.ser;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tinkerpop.gremlin.server.MessageSerializer;
import com.tinkerpop.gremlin.server.ResultCode;
import com.tinkerpop.gremlin.server.message.RequestMessage;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONObjectMapper;
import groovy.json.JsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Serialize results to JSON with version 1.0.x schema.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JsonMessageSerializerV1d0 implements MessageSerializer {
    private static final Logger logger = LoggerFactory.getLogger(JsonMessageSerializerV1d0.class);

    public static final String TOKEN_RESULT = "result";
    public static final String TOKEN_CODE = "code";
    public static final String TOKEN_REQUEST = "requestId";

    /**
     * ObjectMapper instance for JSON serialization via Jackson databind.  Uses custom serializers to write
     * out {@link com.tinkerpop.gremlin.structure.Graph} objects and {@code toString} for unknown objects.
     */
    private static final ObjectMapper mapper = GraphSONObjectMapper.create()
            .customSerializer(new GremlinServerModule()).build();

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{"application/json", "application/vnd.gremlin-v1.0+json"};
    }

    @Override
    public String serialize(final Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (Exception ex) {
            logger.warn("Result [{}] could not be serialized by {}.", o.toString(), JsonMessageSerializerV1d0.class.getName());
            throw new RuntimeException("Error during serialization.", ex);
        }
    }

    @Override
    public String serializeResult(final Optional<Object> o, final ResultCode code, final Optional<RequestMessage> requestMessage) {
        try {
            final Map<String, Object> result = new HashMap<>();
            result.put(TOKEN_CODE, code.getValue());
            result.put(TOKEN_RESULT, o.isPresent() ? o.get() : null);
            result.put(TOKEN_REQUEST, requestMessage.isPresent() ? requestMessage.get().getRequestId() : null);

            return mapper.writeValueAsString(result);
        } catch (Exception ex) {
            logger.warn("Result [{}] could not be serialized by {}.", o.toString(), JsonMessageSerializerV1d0.class.getName());
            throw new RuntimeException("Error during serialization.", ex);
        }
    }

    @Override
    public Optional<RequestMessage> deserializeRequest(final String msg) {
        try {
            return Optional.of(mapper.readValue(msg, RequestMessage.class));
        } catch (Exception ex) {
            logger.warn("The request message [{}] could not be deserialized by {}.", msg, JsonMessageSerializerV1d0.class.getName());
            return Optional.empty();
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
