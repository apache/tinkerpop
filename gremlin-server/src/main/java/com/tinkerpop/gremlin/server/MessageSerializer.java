package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.server.util.ser.JsonMessageSerializerV1d0;
import com.tinkerpop.gremlin.server.util.ser.ToStringMessageSerializer;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * Serializes data to and from Gremlin Server.  Typically the object being serialized or deserialized will be an item
 * from an {@link java.util.Iterator} as returned from the {@code ScriptEngine} or an incoming {@link RequestMessage}.
 * {@link MessageSerializer} instances are instantiated to a cache via {@link ServiceLoader} and indexed based on
 * the mime types they support.  If a mime type is supported more than once, the last {@link MessageSerializer}
 * instance loaded for that mime type is assigned. If a mime type is not found the default
 * {@link com.tinkerpop.gremlin.server.util.ser.ToStringMessageSerializer} is used to return the results.
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
     * Default serializer for results returned from Gremlin Server.
     */
    static final MessageSerializer DEFAULT_RESULT_SERIALIZER = new ToStringMessageSerializer();

    /**
     * Default serializer for requests received by Gremlin Server.
     */
    static final MessageSerializer DEFAULT_REQUEST_SERIALIZER = new JsonMessageSerializerV1d0();

    /**
     * Serialize a result message with a {@link ResultCode#SUCCESS} result code.
     */
    public default String serializeResult(final Object o, final Context context) {
        return this.serializeResult(o, ResultCode.SUCCESS, context);
    }

    /**
     * Serialize a result message.
     *
     * @param o       the result
     * @param code    the response code
     * @param context the context of the server and request
     * @return the result serialized to a String
     */
    public String serializeResult(final Object o, final ResultCode code, final Context context);

    /**
     * Serialize an object.
     */
    public String serialize(final Object o);

    /**
     * Deserialize a {@link RequestMessage} into an object.
     */
    public Optional<RequestMessage> deserializeRequest(final String msg);

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
