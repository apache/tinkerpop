package com.tinkerpop.gremlin.server;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.ser.std.StdKeySerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.tinkerpop.blueprints.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializes a single result from the ScripEngine.  Typically this will be an item from an iterator.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ResultSerializer {
    public static final ToStringResultSerializer TO_STRING_RESULT_SERIALIZER = new ToStringResultSerializer();
    public static final JsonResultSerializer JSON_RESULT_SERIALIZER = new JsonResultSerializer();

    public default String serialize(final Object o, final Context context) {
        return this.serialize(o, ResultCode.SUCCESS, context);
    }

    public String serialize(final Object o, final ResultCode code, final Context context);

    /**
     * Choose a serializer based on the "accept" argument on the message, where "accept" is a mime type.
     */
    public static ResultSerializer select(final String accept) {
        if (accept.equals("application/json"))
            return JSON_RESULT_SERIALIZER;
        else
            return TO_STRING_RESULT_SERIALIZER;
    }

    /**
     * Use toString() to serialize the result.
     */
    public static class ToStringResultSerializer implements ResultSerializer {

        private static final String TEXT_RESPONSE_FORMAT_WITH_RESULT = "%s>>%s";
        private static final String TEXT_RESPONSE_FORMAT_WITH_NULL = "%s>>null";

        @Override
        public String serialize(final Object o, final ResultCode code, final Context context) {
            return o == null ?
                    String.format(TEXT_RESPONSE_FORMAT_WITH_NULL, context.getRequestMessage().requestId) :
                    String.format(TEXT_RESPONSE_FORMAT_WITH_RESULT, context.getRequestMessage().requestId, o.toString());
        }
    }

    /**
     * Converts a result to JSON.
     */
    public static class JsonResultSerializer implements ResultSerializer {
        static final Version JSON_SERIALIZATION_VERSION = new Version(0,1,0,"","com.tinkerpop.gremlin", "gremlin-server");

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

            final DefaultSerializerProvider provider = new GremlinSerializerProvider();
            provider.setDefaultKeySerializer(new GremlinJacksonKeySerializer());

            setSerializerProvider(provider);

            registerModules(new GremlinModule());
        }};

        @Override
        public String serialize(final Object o, final ResultCode code, final Context context) {
            try {
                final Map<String,Object> result = new HashMap<>();
                result.put(TOKEN_CODE, code.getValue());
                result.put(TOKEN_RESULT, o);
                result.put(TOKEN_VERSION, JsonResultSerializer.JSON_SERIALIZATION_VERSION.toString());

                // a context may not be available
                if (context != null)
                    result.put(TOKEN_REQUEST, context.getRequestMessage().requestId);

                return mapper.writeValueAsString(result);
            } catch (Exception ex) {
                throw new RuntimeException("Error during serialization.", ex);
            }
        }

        /**
         * Serializer that converts unknown objects to values via toString().
         */
        public final static class GremlinSerializerProvider extends DefaultSerializerProvider {
            private static final long serialVersionUID = 1L;
            private static final ToStringSerializer toStringSerializer = new ToStringSerializer();

            public GremlinSerializerProvider() {
                super();
            }

            protected GremlinSerializerProvider(final SerializerProvider src,
                           final SerializationConfig config, final SerializerFactory f) {
                super(src, config, f);
            }

            @Override
            public JsonSerializer<Object> getUnknownTypeSerializer(final Class<?> aClass) {
                return toStringSerializer;
            }

            @Override
            public GremlinSerializerProvider createInstance(final SerializationConfig config,
                                       final SerializerFactory jsf) {
                return new GremlinSerializerProvider(this, config, jsf);
            }
        }

        public static class GremlinModule extends SimpleModule {
            public GremlinModule() {
                super("gremlin", JsonResultSerializer.JSON_SERIALIZATION_VERSION);
                addSerializer(Edge.class, new EdgeJacksonSerializer());
                addSerializer(Property.class, new PropertyJacksonSerializer());
                addSerializer(Vertex.class, new VertexJacksonSerializer());
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
}
