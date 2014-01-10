package com.tinkerpop.gremlin.server.util.ser;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdKeySerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.MessageSerializer;
import com.tinkerpop.gremlin.server.RequestMessage;
import com.tinkerpop.gremlin.server.ResultCode;
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

    static final Version JSON_SERIALIZATION_VERSION = new Version(1,0,0,"","com.tinkerpop.gremlin", "gremlin-server");

    public static final String TOKEN_RESULT = "result";
    public static final String TOKEN_ID = "id";
    public static final String TOKEN_TYPE = "type";
    public static final String TOKEN_KEY = "key";
    public static final String TOKEN_VALUE = "value";
    public static final String TOKEN_CODE = "code";
    public static final String TOKEN_PROPERTIES = "properties";
    public static final String TOKEN_META = "meta";
    public static final String TOKEN_EDGE = "edge";
    public static final String TOKEN_VERSION = "version";
    public static final String TOKEN_VERTEX = "vertex";
    public static final String TOKEN_REQUEST = "requestId";
    public static final String TOKEN_IN = "in";
    public static final String TOKEN_OUT = "out";
    public static final String TOKEN_LABEL = "label";

    /**
     * ObjectMapper instance for JSON serialization via Jackson databind.  Uses custom serializers to write
     * out {@link com.tinkerpop.blueprints.Graph} objects and {@code toString} for unknown objects.
     */
    private static final ObjectMapper mapper = new ObjectMapper() {{
        // empty beans should be just toString'd
        disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        // this provider toStrings all unknown classes and converts keys in Map objects that are Object to String.
        final DefaultSerializerProvider provider = new GremlinSerializerProvider();
        provider.setDefaultKeySerializer(new GremlinJacksonKeySerializer());
        setSerializerProvider(provider);

        registerModule(new GremlinModule());

        // plugin external serialization modules
        findAndRegisterModules();
    }};

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
    public String serializeResult(final Object o, final ResultCode code, final Context context) {
        try {
            final Map<String,Object> result = new HashMap<>();
            result.put(TOKEN_CODE, code.getValue());
            result.put(TOKEN_RESULT, o);
            result.put(TOKEN_VERSION, JSON_SERIALIZATION_VERSION.toString());

            // a context may not be available
            if (context != null)
                result.put(TOKEN_REQUEST, context.getRequestMessage().requestId);

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

    /**
     * Serializer mappings for Gremlin/Blueprints classes that will be serialized to JSON.
     */
    public static class GremlinModule extends SimpleModule {
        public GremlinModule() {
            super("gremlin", JsonMessageSerializerV1d0.JSON_SERIALIZATION_VERSION);
            addSerializer(Edge.class, new EdgeJacksonSerializer());
            addSerializer(Property.class, new PropertyJacksonSerializer());
            addSerializer(Vertex.class, new VertexJacksonSerializer());
            addSerializer(JsonBuilder.class, new JsonBuilderJacksonSerializer());
        }
    }

    public static class EdgeJacksonSerializer extends StdSerializer<Edge> {
        public EdgeJacksonSerializer() {
            super(Edge.class);
        }

        @Override
        public void serialize(final Edge edge, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField(TOKEN_ID, edge.getId());
            jsonGenerator.writeStringField(TOKEN_LABEL, edge.getLabel());
            jsonGenerator.writeStringField(TOKEN_TYPE, TOKEN_EDGE);
            jsonGenerator.writeObjectField(TOKEN_IN, edge.getVertex(Direction.IN).getId());
            jsonGenerator.writeObjectField(TOKEN_OUT, edge.getVertex(Direction.OUT).getId());
            jsonGenerator.writeObjectField(TOKEN_PROPERTIES, edge.getProperties());
            jsonGenerator.writeEndObject();
        }
    }


    public static class VertexJacksonSerializer extends StdSerializer<Vertex> {
        public VertexJacksonSerializer() {
            super(Vertex.class);
        }

        @Override
        public void serialize(final Vertex vertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField(TOKEN_ID, vertex.getId());
            jsonGenerator.writeStringField(TOKEN_LABEL, vertex.getLabel());
            jsonGenerator.writeStringField(TOKEN_TYPE, TOKEN_VERTEX);
            jsonGenerator.writeObjectField(TOKEN_PROPERTIES, vertex.getProperties());
            jsonGenerator.writeEndObject();
        }
    }

    public static class PropertyJacksonSerializer extends StdSerializer<Property> {
        public PropertyJacksonSerializer() {
            super(Property.class);
        }

        @Override
        public void serialize(final Property property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField(TOKEN_VALUE, property.get());
            jsonGenerator.writeEndObject();
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

    /**
     * Maps in the JVM can have {@link Object} as a key, but in JSON they must be a {@link String}.
     */
    public static class GremlinJacksonKeySerializer extends StdKeySerializer {
        @Override
        public void serialize(Object o, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
            if (Element.class.isAssignableFrom(o.getClass()))
                jsonGenerator.writeFieldName((((Element) o).getId()).toString());
            else
                super.serialize(o, jsonGenerator, serializerProvider);
        }
    }
}
