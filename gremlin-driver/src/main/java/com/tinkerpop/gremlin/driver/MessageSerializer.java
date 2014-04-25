package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.ser.JsonMessageSerializerV1d0;
import com.tinkerpop.gremlin.driver.ser.SerializationException;
import com.tinkerpop.gremlin.util.StreamFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * Serializes data to and from Gremlin Server.  Typically the object being serialized or deserialized will be an item
 * from an {@link java.util.Iterator} as returned from the {@code ScriptEngine} or an incoming {@link com.tinkerpop.gremlin.driver.message.RequestMessage}.
 * {@link MessageSerializer} instances are instantiated to a cache via {@link ServiceLoader} and indexed based on
 * the mime types they support.  If a mime type is supported more than once, the last {@link MessageSerializer}
 * instance loaded for that mime type is assigned. If a mime type is not found the default
 * {@link com.tinkerpop.gremlin.driver.ser.JsonMessageSerializerV1d0} is used to return the results.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface MessageSerializer {

    static final Logger logger = LoggerFactory.getLogger(MessageSerializer.class);

    /**
     * Map of serializers to mime types. Initialize {@link MessageSerializer} instances with {@link ServiceLoader}
     * invoking {@link #mimeTypesSupported()} and mapping each mime type returned in that array back to the associated
     * {@link MessageSerializer} in the @{link Map},
     */
    static final Map<String, MessageSerializer> serializers = new HashMap<String, MessageSerializer>() {{
        final ServiceLoader<MessageSerializer> serviceLoader = ServiceLoader.load(MessageSerializer.class);
        StreamFactory.stream(serviceLoader.iterator()).flatMap(serializer ->
                Stream.of(serializer.mimeTypesSupported()).map(mimeType -> Arrays.asList(mimeType, serializer))
        ).forEach(l -> put(l.get(0).toString(), (MessageSerializer) l.get(1)));
    }};

    /**
     * Default serializer for results returned from Gremlin Server. This implementation must be of type
     * {@link com.tinkerpop.gremlin.driver.ser.MessageTextSerializer} so that it can be compatible with text-based
     * websocket messages.
     */
    static final MessageSerializer DEFAULT_RESULT_SERIALIZER = new JsonMessageSerializerV1d0();

    /**
     * Default serializer for requests received by Gremlin Server. This implementation must be of type
     * {@link com.tinkerpop.gremlin.driver.ser.MessageTextSerializer} so that it can be compatible with text-based
     * websocket messages.
     */
    static final MessageSerializer DEFAULT_REQUEST_SERIALIZER = new JsonMessageSerializerV1d0();

    /**
     * Serialize a {@link ResponseMessage} to a Netty {@code ByteBuf}.
     *
     * @param responseMessage The response message to serialize to bytes.
     * @param allocator The Netty allocator for the {@code ByteBuf} to return back.
     */
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException;

    /**
     * Serialize a {@link ResponseMessage} to a Netty {@code ByteBuf}.
     *
     * @param requestMessage The request message to serialize to bytes.
     * @param allocator The Netty allocator for the {@code ByteBuf} to return back.
     */
    public ByteBuf serializeRequestAsBinary(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException;

    /**
     * Deserialize a Netty {@code ByteBuf} into a {@link RequestMessage}.
     */
    public RequestMessage deserializeRequest(final ByteBuf msg) throws SerializationException;

    /**
     * Deserialize a Netty {@code ByteBuf} into a {@link ResponseMessage}.
     */
    public ResponseMessage deserializeResponse(final ByteBuf msg) throws SerializationException;

    /**
     * The list of mime types that the serializer supports.
     */
    public String[] mimeTypesSupported();

    /**
     * Choose a serializer based on the mimetype.
     */
    public static MessageSerializer select(final String mimeType, final MessageSerializer defaultSerializer) {
        if (logger.isWarnEnabled() && !serializers.containsKey(mimeType))
            logger.warn("Gremlin Server is not configured with a serializer for the requested mime type [{}] - using {} by default",
                    mimeType, defaultSerializer.getClass().getName());

        return serializers.getOrDefault(mimeType, defaultSerializer);
    }
}
