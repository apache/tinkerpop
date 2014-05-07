package com.tinkerpop.gremlin.driver.ser;

import com.tinkerpop.gremlin.driver.MessageSerializer;

/**
 * An enum of the default serializers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum Serializers {
    JSON(SerTokens.MIME_JSON),
    JSON_V1D0(SerTokens.MIME_JSON_V1D0),
    KRYO_V1D0(SerTokens.MIME_KRYO_V1D0);

    private String value;

    /**
     * Default serializer for results returned from Gremlin Server. This implementation must be of type
     * {@link com.tinkerpop.gremlin.driver.ser.MessageTextSerializer} so that it can be compatible with text-based
     * websocket messages.
     */
    public static final MessageSerializer DEFAULT_RESULT_SERIALIZER = new JsonMessageSerializerV1d0();

    /**
     * Default serializer for requests received by Gremlin Server. This implementation must be of type
     * {@link com.tinkerpop.gremlin.driver.ser.MessageTextSerializer} so that it can be compatible with text-based
     * websocket messages.
     */
    public static final MessageSerializer DEFAULT_REQUEST_SERIALIZER = new JsonMessageSerializerV1d0();

    private Serializers(final String mimeType) {
        this.value = mimeType;
    }

    public String getValue() {
        return value;
    }

    public MessageSerializer simpleInstance() {
        switch (value) {
            case SerTokens.MIME_JSON:
                return new JsonMessageSerializerV1d0();
            case SerTokens.MIME_JSON_V1D0:
                return new JsonMessageSerializerGremlinV1d0();
            case SerTokens.MIME_KRYO_V1D0:
                return new KryoMessageSerializerV1d0();
            default:
                throw new RuntimeException("Could not create a simple MessageSerializer instance of " + value);
        }
    }
}
