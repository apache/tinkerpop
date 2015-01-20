package com.tinkerpop.gremlin.driver.ser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
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
     * ObjectMapper instance for JSON serialization via Jackson databind.  Uses mapper serializers to write
     * out {@link com.tinkerpop.gremlin.structure.Graph} objects and {@code toString} for unknown objects.
     */
    protected static final ObjectMapper mapper = GraphSONMapper.build()
            .addCustomModule(new GremlinServerModule())
            .embedTypes(false)
            .create().createMapper();

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

    @Override
    public String serializeResponseAsString(final ResponseMessage responseMessage) throws SerializationException {
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

            return obtainMapper().writeValueAsString(message);
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
