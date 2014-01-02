package com.tinkerpop.gremlin.server;

import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.server.util.ser.ToStringResultSerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * Serializes a single result from the {@code ScriptEngine}.  Typically this will be an item from an
 * {@link java.util.Iterator}.  {@link ResultSerializer} instances are instantiated to a cache via
 * {@link ServiceLoader} and indexed based on the mime types they support.  If a mime type is supported more than
 * once, the last {@link ResultSerializer} instance loaded for that mime type is assigned. If a mime type is not
 * found the default {@link ToStringResultSerializer} is used to return the results.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ResultSerializer {
    /**
     * Map of serializers to mime types. Initialize {@link ResultSerializer} instances with {@link ServiceLoader}
     * invoking {@link #mimeTypesSupported()} and mapping each mime type returned in that array back to the associated
     * {@link ResultSerializer} in the @{link Map},
     */
    static final Map<String, ResultSerializer> resultSerializers = new HashMap<String, ResultSerializer>(){{
        final ServiceLoader<ResultSerializer> serviceLoader = ServiceLoader.load(ResultSerializer.class);
        StreamFactory.stream(serviceLoader.iterator()).flatMap(serializer->
                Stream.of(serializer.mimeTypesSupported()).map(mimeType->Arrays.asList(mimeType, serializer))
        ).forEach(l -> put(l.get(0).toString(), (ResultSerializer) l.get(1)));
    }};

    /**
     * Default serializer.
     */
    static final ToStringResultSerializer TO_STRING_RESULT_SERIALIZER = new ToStringResultSerializer();

    /**
     * Serialize a result with a {@link ResultCode#SUCCESS} result code.
     */
    public default String serialize(final Object o, final Context context) {
        return this.serialize(o, ResultCode.SUCCESS, context);
    }

    /**
     * Serialize a result.
     *
     * @param o the result
     * @param code the response code
     * @param context the context of the server and request
     * @return the result serialized to a String
     */
    public String serialize(final Object o, final ResultCode code, final Context context);

    /**
     * The list of mime types that the serializer supports.
     */
    public String[] mimeTypesSupported();

    /**
     * Choose a serializer based on the "accept" argument on the message, where "accept" is a mime type.
     */
    public static ResultSerializer select(final String accept) {
        return resultSerializers.getOrDefault(accept, TO_STRING_RESULT_SERIALIZER);
    }
}
