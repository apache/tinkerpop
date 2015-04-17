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
package org.apache.tinkerpop.gremlin.driver.ser;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.javatuples.Pair;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GryoMessageSerializerV1d0 implements MessageSerializer {
    private GryoMapper gryoMapper;
    private ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            return gryoMapper.createMapper();
        }
    };

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final String MIME_TYPE = SerTokens.MIME_GRYO_V1D0;
    private static final String MIME_TYPE_STRINGD = SerTokens.MIME_GRYO_V1D0 + "-stringd";

    private static final String TOKEN_CUSTOM = "custom";
    private static final String TOKEN_SERIALIZE_RESULT_TO_STRING = "serializeResultToString";
    private static final String TOKEN_USE_MAPPER_FROM_GRAPH = "useMapperFromGraph";

    private boolean serializeToString;

    /**
     * Creates an instance with a standard {@link org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper} instance. Note that this instance
     * will be overriden by {@link #configure} is called.
     */
    public GryoMessageSerializerV1d0() {
        gryoMapper = GryoMapper.build().create();
    }

    /**
     * Creates an instance with a provided mapper configured {@link org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper} instance. Note that this instance
     * will be overriden by {@link #configure} is called.
     */
    public GryoMessageSerializerV1d0(final GryoMapper kryo) {
        this.gryoMapper = kryo;
    }

    @Override
    public void configure(final Map<String, Object> config, final Map<String, Graph> graphs) {
        final GryoMapper.Builder builder;
        final Object graphToUseForMapper = config.get(TOKEN_USE_MAPPER_FROM_GRAPH);
        if (graphToUseForMapper != null) {
            if (null == graphs) throw new IllegalStateException(String.format(
                    "No graphs have been provided to the serializer and therefore %s is not a valid configuration", TOKEN_USE_MAPPER_FROM_GRAPH));

            final Graph g = graphs.get(graphToUseForMapper.toString());
            if (null == g) throw new IllegalStateException(String.format(
                    "There is no graph named [%s] configured to be used in the %s setting",
                    graphToUseForMapper, TOKEN_USE_MAPPER_FROM_GRAPH));

            // a graph was found so use the mapper it constructs.  this allows gryo to be auto-configured with any
            // custom classes that the implementation allows for
            builder = g.io().gryoMapper();
        } else {
            // no graph was supplied so just use the default - this will likely be the case when using a graph
            // with no custom classes or a situation where the user needs complete control like when using two
            // distinct implementations each with their own custom classes.
            builder = GryoMapper.build();
        }

        final List<String> classNameList;
        try {
            classNameList = (List<String>) config.getOrDefault(TOKEN_CUSTOM, new ArrayList<String>());
        } catch (Exception ex) {
            throw new IllegalStateException(String.format("Invalid configuration value of [%s] for [%s] setting on %s serialization configuration",
                    config.getOrDefault(TOKEN_CUSTOM, ""), TOKEN_CUSTOM, this.getClass().getName()), ex);
        }

        if (!classNameList.isEmpty()) {
            final List<Pair<Class, Function<Kryo, Serializer>>> classList = classNameList.stream().map(serializerDefinition -> {
                String className;
                Optional<String> serializerName;
                if (serializerDefinition.contains(";")) {
                    final String[] split = serializerDefinition.split(";");
                    if (split.length != 2)
                        throw new IllegalStateException(String.format("Invalid format for serializer definition [%s] - expected <class>:<serializer-class>", serializerDefinition));

                    className = split[0];
                    serializerName = Optional.of(split[1]);
                } else {
                    serializerName = Optional.empty();
                    className = serializerDefinition;
                }

                try {
                    final Class clazz = Class.forName(className);
                    final Serializer serializer;
                    if (serializerName.isPresent()) {
                        final Class serializerClazz = Class.forName(serializerName.get());
                        serializer = (Serializer) serializerClazz.newInstance();
                    } else
                        serializer = null;

                    return Pair.<Class, Function<Kryo, Serializer>>with(clazz, kryo -> serializer);
                } catch (Exception ex) {
                    throw new IllegalStateException("Class could not be found", ex);
                }
            }).collect(Collectors.toList());

            classList.forEach(c -> builder.addCustom(c.getValue0(), (Function) c.getValue1()));
        }

        this.serializeToString = Boolean.parseBoolean(config.getOrDefault(TOKEN_SERIALIZE_RESULT_TO_STRING, "false").toString());

        this.gryoMapper = builder.create();
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{this.serializeToString ? MIME_TYPE_STRINGD : MIME_TYPE};
    }

    @Override
    public ResponseMessage deserializeResponse(final ByteBuf msg) throws SerializationException {
        try {
            final Kryo kryo = kryoThreadLocal.get();
            final byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);
            try (final Input input = new Input(payload)) {
                final UUID requestId = kryo.readObjectOrNull(input, UUID.class);
                final int status = input.readShort();
                final String statusMsg = input.readString();
                final Map<String,Object> statusAttributes = (Map<String,Object>) kryo.readClassAndObject(input);
                final Object result = kryo.readClassAndObject(input);
                final Map<String,Object> metaAttributes = (Map<String,Object>) kryo.readClassAndObject(input);

                return ResponseMessage.build(requestId)
                        .code(ResponseStatusCode.getFromValue(status))
                        .statusMessage(statusMsg)
                        .statusAttributes(statusAttributes)
                        .result(result)
                        .responseMetaData(metaAttributes)
                        .create();
            }
        } catch (Exception ex) {
            logger.warn("Response [{}] could not be deserialized by {}.", msg, GryoMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final Kryo kryo = kryoThreadLocal.get();
            try (final OutputStream baos = new ByteArrayOutputStream()) {
                final Output output = new Output(baos);

                // request id - if present
                kryo.writeObjectOrNull(output, responseMessage.getRequestId() != null ? responseMessage.getRequestId() : null, UUID.class);

                // status
                output.writeShort(responseMessage.getStatus().getCode().getValue());
                output.writeString(responseMessage.getStatus().getMessage());
                kryo.writeClassAndObject(output, responseMessage.getStatus().getAttributes());

                // result
                kryo.writeClassAndObject(output, serializeToString ? serializeResultToString(responseMessage) : responseMessage.getResult().getData());
                kryo.writeClassAndObject(output, responseMessage.getResult().getMeta());

                final long size = output.total();
                if (size > Integer.MAX_VALUE)
                    throw new SerializationException(String.format("Message size of %s exceeds allocatable space", size));

                encodedMessage = allocator.buffer((int) output.total());
                encodedMessage.writeBytes(output.toBytes());
            }

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn("Response [{}] could not be serialized by {}.", responseMessage.toString(), GryoMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public RequestMessage deserializeRequest(final ByteBuf msg) throws SerializationException {
        try {
            final Kryo kryo = kryoThreadLocal.get();
            final byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);
            try (final Input input = new Input(payload)) {
                // by the time the message gets here, the mime length/type have been already read, so this part just
                // needs to process the payload.
                final UUID id = kryo.readObject(input, UUID.class);
                final String processor = input.readString();
                final String op = input.readString();

                final RequestMessage.Builder builder = RequestMessage.build(op)
                        .overrideRequestId(id)
                        .processor(processor);

                final Map<String, Object> args = kryo.readObject(input, HashMap.class);
                args.forEach(builder::addArg);
                return builder.create();
            }
        } catch (Exception ex) {
            logger.warn("Request [{}] could not be deserialized by {}.", msg, GryoMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public ByteBuf serializeRequestAsBinary(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final Kryo kryo = kryoThreadLocal.get();
            try (final OutputStream baos = new ByteArrayOutputStream()) {
                final Output output = new Output(baos);
                final String mimeType = serializeToString ? MIME_TYPE_STRINGD : MIME_TYPE;
                output.writeByte(mimeType.length());
                output.write(mimeType.getBytes(UTF8));

                kryo.writeObject(output, requestMessage.getRequestId());
                output.writeString(requestMessage.getProcessor());
                output.writeString(requestMessage.getOp());
                kryo.writeObject(output, requestMessage.getArgs());

                final long size = output.total();
                if (size > Integer.MAX_VALUE)
                    throw new SerializationException(String.format("Message size of %s exceeds allocatable space", size));

                encodedMessage = allocator.buffer((int) size);
                encodedMessage.writeBytes(output.toBytes());
            }

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn("Request [{}] could not be serialized by {}.", requestMessage.toString(), GryoMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    private Object serializeResultToString(final ResponseMessage msg) {
        if (msg.getResult() == null) return "null";
        if (msg.getResult().getData() == null) return "null";

        // the IteratorHandler should return a collection so keep it as such
        final Object o = msg.getResult().getData();
        if (o instanceof Collection) {
            return ((Collection) o).stream().map(d -> null == d ? "null" : d.toString()).collect(Collectors.toList());
        } else {
            return o.toString();
        }
    }
}
