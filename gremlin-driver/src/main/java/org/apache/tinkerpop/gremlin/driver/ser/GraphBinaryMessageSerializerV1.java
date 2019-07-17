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
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryIo;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.driver.ser.binary.RequestMessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.ResponseMessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.CustomTypeSerializer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.javatuples.Pair;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class GraphBinaryMessageSerializerV1 extends AbstractMessageSerializer {

    public static final String TOKEN_CUSTOM = "custom";
    public static final String TOKEN_BUILDER = "builder";
    public static final String TOKEN_SERIALIZE_RESULT_TO_STRING = "serializeResultToString";

    private static final String MIME_TYPE = SerTokens.MIME_GRAPHBINARY_V1D0;
    private static final String MIME_TYPE_STRINGD = SerTokens.MIME_GRAPHBINARY_V1D0 + "-stringd";

    private byte[] header = MIME_TYPE.getBytes(UTF_8);
    private boolean serializeToString = false;
    private GraphBinaryReader reader;
    private GraphBinaryWriter writer;
    private RequestMessageSerializer requestSerializer;
    private ResponseMessageSerializer responseSerializer;

    /**
     * Creates a new instance of the message serializer using the default type serializers.
     */
    public GraphBinaryMessageSerializerV1() {
        this(TypeSerializerRegistry.INSTANCE);
    }

    public GraphBinaryMessageSerializerV1(final TypeSerializerRegistry registry) {
        reader = new GraphBinaryReader(registry);
        writer = new GraphBinaryWriter(registry);

        requestSerializer = new RequestMessageSerializer();
        responseSerializer = new ResponseMessageSerializer();
    }

    public GraphBinaryMessageSerializerV1(final TypeSerializerRegistry.Builder builder) {
        this(builder.create());
    }

    @Override
    public void configure(final Map<String, Object> config, final Map<String, Graph> graphs) {
        final String builderClassName = (String) config.get(TOKEN_BUILDER);
        final TypeSerializerRegistry.Builder builder;

        if (builderClassName != null) {
            try {
                final Class<?> clazz = Class.forName(builderClassName);
                final Constructor<?> ctor = clazz.getConstructor();
                builder = (TypeSerializerRegistry.Builder) ctor.newInstance();
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        } else {
            builder = TypeSerializerRegistry.build();
        }

        final List<String> classNameList = getListStringFromConfig(TOKEN_IO_REGISTRIES, config);
        classNameList.forEach(className -> {
            try {
                final Class<?> clazz = Class.forName(className);
                try {
                    final Method instanceMethod = tryInstanceMethod(clazz);
                    final IoRegistry ioreg = (IoRegistry) instanceMethod.invoke(null);
                    final List<Pair<Class, CustomTypeSerializer>> classSerializers = ioreg.find(GraphBinaryIo.class, CustomTypeSerializer.class);
                    for (Pair<Class,CustomTypeSerializer> cs : classSerializers) {
                        builder.addCustomType(cs.getValue0(), cs.getValue1());
                    }
                } catch (Exception methodex) {
                    throw new IllegalStateException(String.format("Could not instantiate IoRegistry from an instance() method on %s", className), methodex);
                }
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });

        addCustomClasses(config, builder);

        this.serializeToString = Boolean.parseBoolean(config.getOrDefault(TOKEN_SERIALIZE_RESULT_TO_STRING, "false").toString());
        this.header = this.serializeToString ? MIME_TYPE_STRINGD.getBytes(UTF_8) : MIME_TYPE.getBytes(UTF_8);

        final TypeSerializerRegistry registry = builder.create();
        reader = new GraphBinaryReader(registry);
        writer = new GraphBinaryWriter(registry);

        requestSerializer = new RequestMessageSerializer();
        responseSerializer = new ResponseMessageSerializer();
    }

    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        final ByteBuf buffer = allocator.buffer();

        try {
            final ResponseMessage msgToWrite = !serializeToString ? responseMessage :
                    ResponseMessage.build(responseMessage.getRequestId())
                            .code(responseMessage.getStatus().getCode())
                            .statusAttributes(responseMessage.getStatus().getAttributes())
                            .responseMetaData(responseMessage.getResult().getMeta())
                            .result(serializeResultToString(responseMessage))
                            .statusMessage(responseMessage.getStatus().getMessage()).create();

            responseSerializer.writeValue(msgToWrite, buffer, writer);
        } catch (Exception ex) {
            buffer.release();
            throw ex;
        }

        return buffer;
    }

    @Override
    public ByteBuf serializeRequestAsBinary(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        final ByteBuf buffer = allocator.buffer().writeByte(header.length).writeBytes(header);

        try {
            requestSerializer.writeValue(requestMessage, buffer, writer);
        } catch (Exception ex) {
            buffer.release();
            throw ex;
        }

        return buffer;
    }

    @Override
    public RequestMessage deserializeRequest(final ByteBuf msg) throws SerializationException {
        return requestSerializer.readValue(msg, reader);
    }

    @Override
    public ResponseMessage deserializeResponse(final ByteBuf msg) throws SerializationException {
        return responseSerializer.readValue(msg, reader);
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{serializeToString ? MIME_TYPE_STRINGD : MIME_TYPE};
    }

    private void addCustomClasses(final Map<String, Object> config, final TypeSerializerRegistry.Builder builder) {
        final List<String> classNameList = getListStringFromConfig(TOKEN_CUSTOM, config);

        classNameList.forEach(serializerDefinition -> {
            final String className;
            final String serializerName;
            if (serializerDefinition.contains(";")) {
                final String[] split = serializerDefinition.split(";");
                if (split.length != 2)
                    throw new IllegalStateException(String.format("Invalid format for serializer definition [%s] - expected <class>;<serializer-class>", serializerDefinition));

                className = split[0];
                serializerName = split[1];
            } else {
                throw new IllegalStateException(String.format("Invalid format for serializer definition [%s] - expected <class>;<serializer-class>", serializerDefinition));
            }

            try {
                final Class clazz = Class.forName(className);
                final Class serializerClazz = Class.forName(serializerName);
                final CustomTypeSerializer serializer = (CustomTypeSerializer) serializerClazz.newInstance();
                builder.addCustomType(clazz, serializer);
            } catch (Exception ex) {
                throw new IllegalStateException("CustomTypeSerializer could not be instantiated", ex);
            }
        });
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
