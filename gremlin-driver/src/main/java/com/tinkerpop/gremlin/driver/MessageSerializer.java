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
}
