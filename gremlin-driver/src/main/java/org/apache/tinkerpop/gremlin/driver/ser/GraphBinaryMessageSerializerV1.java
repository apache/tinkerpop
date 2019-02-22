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
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class GraphBinaryMessageSerializerV1 extends AbstractMessageSerializer {
    public static final String TOKEN_CUSTOM = "custom";
    public static final String TOKEN_BUILDER = "builder";
    private static final String MIME_TYPE = SerTokens.MIME_GRAPHBINARY_V1D0;
    private static final byte[] HEADER = MIME_TYPE.getBytes(UTF_8);

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
                Class<?> clazz = Class.forName(builderClassName);
                Constructor<?> ctor = clazz.getConstructor();
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
            responseSerializer.writeValue(responseMessage, buffer, writer);
        } catch (Exception ex) {
            buffer.release();
            throw ex;
        }

        return buffer;
    }

    @Override
    public ByteBuf serializeRequestAsBinary(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        final ByteBuf buffer = allocator.buffer().writeByte(HEADER.length).writeBytes(HEADER);

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
        return new String[] { MIME_TYPE };
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
}
