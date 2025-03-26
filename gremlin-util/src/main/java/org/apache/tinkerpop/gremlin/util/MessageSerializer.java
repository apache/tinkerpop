/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.util;

import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Serializes data to and from Gremlin Server.  Typically, the object being serialized or deserialized will be an item
 * from an {@link Iterator} as returned from the {@code ScriptEngine} or an incoming {@link RequestMessage}.
 * {@link MessageSerializer} instances are instantiated to a cache via {@link ServiceLoader} and indexed based on
 * the mime types they support.  If a mime type is supported more than once, the first {@link MessageSerializer}
 * instance loaded for that mime type is assigned. If a mime type is not found the server default is chosen. The
 * default may change from version to version so it is best to not rely on it when developing applications and to
 * always be explicit in specifying the type you wish to bind to.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface MessageSerializer<M> {

    static final Logger logger = LoggerFactory.getLogger(MessageSerializer.class);

    /**
     * Gets the "mapper" that performs the underlying serialization work.
     */
    M getMapper();

    /**
     * Serialize a {@link ResponseMessage} to a Netty {@code ByteBuf}.
     *
     * @param responseMessage The response message to serialize to bytes.
     * @param allocator       The Netty allocator for the {@code ByteBuf} to return back.
     */
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException;

    /**
     * Serialize a {@link ResponseMessage} to a Netty {@code ByteBuf}.
     *
     * @param requestMessage The request message to serialize to bytes.
     * @param allocator      The Netty allocator for the {@code ByteBuf} to return back.
     */
    public ByteBuf serializeRequestAsBinary(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException;

    /**
     * Deserialize a Netty {@code ByteBuf} into a {@link RequestMessage}.
     */
    public RequestMessage deserializeBinaryRequest(final ByteBuf msg) throws SerializationException;

    /**
     * Deserialize a Netty {@code ByteBuf} into a {@link RequestMessage}.
     */
    public ResponseMessage deserializeBinaryResponse(final ByteBuf msg) throws SerializationException;

    /**
     * The list of mime types that the serializer supports. They should be ordered in preferred ordered where the
     * greatest fidelity match is first.
     */
    public String[] mimeTypesSupported();

    /**
     * Configure the serializer with mapper settings as required.  The default implementation does not perform any
     * function and it is up to the interface implementation to determine how the configuration will be executed
     * and what its requirements are.  An implementation may choose to use the list of available graphs to help
     * initialize a serializer.  The implementation should account for the possibility of a null value being
     * provided for that parameter.
     */
    public default void configure(final Map<String, Object> config, final Map<String, Graph> graphs) {
    }

    public ByteBuf writeHeader(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException;

    public ByteBuf writeChunk(final Object aggregate, final ByteBufAllocator allocator) throws SerializationException;

    public ByteBuf writeFooter(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException;

    public ByteBuf writeErrorFooter(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException;

    public ResponseMessage readChunk(final ByteBuf byteBuf, final boolean isFirstChunk) throws SerializationException;

    public enum MessageParts {
        HEADER, DATA, FOOTER;

        public static final EnumSet<MessageParts> ALL = EnumSet.of(HEADER, DATA, FOOTER);
        public static final EnumSet<MessageParts> START = EnumSet.of(HEADER, DATA);
        public static final EnumSet<MessageParts> CHUNK = EnumSet.of(DATA);
        public static final EnumSet<MessageParts> END = EnumSet.of(DATA, FOOTER);
        public static final EnumSet<MessageParts> ERROR = EnumSet.of(FOOTER);
    }

}
