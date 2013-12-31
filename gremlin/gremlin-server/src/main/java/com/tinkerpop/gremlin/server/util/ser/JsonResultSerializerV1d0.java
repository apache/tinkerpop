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
import com.tinkerpop.gremlin.server.ResultCode;
import com.tinkerpop.gremlin.server.ResultSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Serialize results to JSON with version 1.0.x schema.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JsonResultSerializerV1d0 implements ResultSerializer {
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
     * out Graph objects and toString for unknown objects.
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
    public String serialize(final Object o, final ResultCode code, final Context context) {
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
            throw new RuntimeException("Error during serialization.", ex);
        }
    }

    /**
     * Serializer mappings for Gremlin/Blueprints classes that will be serialized to JSON.
     */
    public static class GremlinModule extends SimpleModule {
        public GremlinModule() {
            super("gremlin", JsonResultSerializerV1d0.JSON_SERIALIZATION_VERSION);
            addSerializer(Edge.class, new JsonResultSerializerV1d0.EdgeJacksonSerializer());
            addSerializer(Property.class, new JsonResultSerializerV1d0.PropertyJacksonSerializer());
            addSerializer(Vertex.class, new JsonResultSerializerV1d0.VertexJacksonSerializer());
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
            jsonGenerator.writeObjectField(TOKEN_VALUE, property.getValue());
            jsonGenerator.writeEndObject();
        }
    }

    /**
     * Maps in the JVM can have Object as a key, but in JSON they must be a String.
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
