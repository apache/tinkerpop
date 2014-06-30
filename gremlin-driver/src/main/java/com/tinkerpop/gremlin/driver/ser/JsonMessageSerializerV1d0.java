package com.tinkerpop.gremlin.driver.ser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.driver.message.ResultType;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Serialize results to JSON with version 1.0.x schema.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JsonMessageSerializerV1d0 extends AbstractJsonMessageSerializerV1d0 implements MessageTextSerializer {
    private static final Logger logger = LoggerFactory.getLogger(JsonMessageSerializerV1d0.class);
    private static final String MIME_TYPE = SerTokens.MIME_JSON;

    /**
     * ObjectMapper instance for JSON serialization via Jackson databind.  Uses custom serializers to write
     * out {@link com.tinkerpop.gremlin.structure.Graph} objects and {@code toString} for unknown objects.
     */
    protected static final ObjectMapper mapper = GraphSONObjectMapper.create()
            .customModule(new GremlinServerModule())
            .embedTypes(false)
            .build();

    private static byte[] header;

    static {
        final ByteBuffer buffer = ByteBuffer.allocate(MIME_TYPE.length() + 1);
        buffer.put((byte) MIME_TYPE.length());
        buffer.put(MIME_TYPE.getBytes());
        header = buffer.array();
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{MIME_TYPE};
    }

    @Override
    ObjectMapper obtainMapper() {
        return mapper;
    }

    @Override
    byte[] obtainHeader() {
        return header;
    }

    @Override
    public ResponseMessage deserializeResponse(final String msg) throws SerializationException {
        try {
            final Map<String, Object> responseData = obtainMapper().readValue(msg, mapTypeReference);
            return ResponseMessage.create(UUID.fromString(responseData.get(SerTokens.TOKEN_REQUEST).toString()))
                    .code(ResultCode.getFromValue((Integer) responseData.get(SerTokens.TOKEN_CODE)))
                    .result(responseData.get(SerTokens.TOKEN_RESULT))
                    .contents(ResultType.getFromValue((Integer) responseData.get(SerTokens.TOKEN_TYPE)))
                    .build();
        } catch (Exception ex) {
            logger.warn("Response [{}] could not be deserialized by {}.", msg, AbstractJsonMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public String serializeResponseAsString(final ResponseMessage responseMessage) throws SerializationException {
        try {
            final Map<String, Object> result = new HashMap<>();
            result.put(SerTokens.TOKEN_CODE, responseMessage.getCode().getValue());
            result.put(SerTokens.TOKEN_RESULT, responseMessage.getResult());
            result.put(SerTokens.TOKEN_REQUEST, responseMessage.getRequestId() != null ? responseMessage.getRequestId() : null);
            result.put(SerTokens.TOKEN_TYPE, responseMessage.getResultType().getValue());

            return obtainMapper().writeValueAsString(result);
        } catch (Exception ex) {
            logger.warn("Response [{}] could not be serialized by {}.", responseMessage.toString(), AbstractJsonMessageSerializerV1d0.class.getName());
            throw new RuntimeException("Error during serialization.", ex);
        }
    }

    @Override
    public RequestMessage deserializeRequest(final String msg) throws SerializationException {
        try {
            return obtainMapper().readValue(msg, RequestMessage.class);
        } catch (Exception ex) {
            logger.warn("Request [{}] could not be deserialized by {}.", msg, AbstractJsonMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public String serializeRequestAsString(final RequestMessage requestMessage) throws SerializationException {
        try {
            return obtainMapper().writeValueAsString(requestMessage);
        } catch (Exception ex) {
            logger.warn("Request [{}] could not be serialized by {}.", requestMessage.toString(), AbstractJsonMessageSerializerV1d0.class.getName());
            throw new RuntimeException("Error during serialization.", ex);
        }
    }
}
