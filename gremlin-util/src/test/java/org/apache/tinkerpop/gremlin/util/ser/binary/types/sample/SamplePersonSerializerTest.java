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
package org.apache.tinkerpop.gremlin.util.ser.binary.types.sample;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryIo;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.util.MockitoHamcrestMatcherAdapter.reflectionEquals;
import static org.apache.tinkerpop.gremlin.util.ser.AbstractMessageSerializer.TOKEN_IO_REGISTRIES;
import static org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4.TOKEN_CUSTOM;
import static org.hamcrest.MatcherAssert.assertThat;

public class SamplePersonSerializerTest {

    private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    @Test
    public void shouldCustomSerializationWithPerson() throws IOException {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4(
                TypeSerializerRegistry.build().addCustomType(SamplePerson.class, new SamplePersonSerializer()).create());
        assertPerson(serializer);
    }

    @Test
    public void shouldSerializePersonViaIoRegistry() throws IOException {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        final Map<String,Object> config = new HashMap<>();
        config.put(TOKEN_IO_REGISTRIES, Collections.singletonList(CustomIoRegistry.class.getName()));
        serializer.configure(config, Collections.emptyMap());

        assertPerson(serializer);
    }

    @Test
    public void shouldSerializePersonViaCustom() throws IOException {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        final Map<String,Object> config = new HashMap<>();
        config.put(TOKEN_CUSTOM, Collections.singletonList(String.format("%s;%s",
                SamplePerson.class.getCanonicalName(), SamplePersonSerializer.class.getCanonicalName())));
        serializer.configure(config, Collections.emptyMap());

        assertPerson(serializer);
    }

    @Test
    public void readValueAndWriteValueShouldBeSymmetric() throws IOException {
        final TypeSerializerRegistry registry = TypeSerializerRegistry.build()
                .addCustomType(SamplePerson.class, new SamplePersonSerializer()).create();
        final GraphBinaryReader reader = new GraphBinaryReader(registry);
        final GraphBinaryWriter writer = new GraphBinaryWriter(registry);

        final SamplePerson person = new SamplePerson("Matias",
                OffsetDateTime.of(LocalDateTime.of(2005, 8, 5, 1, 0), ZoneOffset.UTC));

        for (boolean nullable: new boolean[] { true, false }) {
            final Buffer buffer = bufferFactory.create(allocator.buffer());
            writer.writeValue(person, buffer, nullable);
            final SamplePerson actual = reader.readValue(buffer, SamplePerson.class, nullable);

            assertThat(actual, reflectionEquals(person));
            buffer.release();
        }
    }

    private void assertPerson(final GraphBinaryMessageSerializerV4 serializer) throws IOException {
        final OffsetDateTime birthDate = OffsetDateTime.of(LocalDateTime.of(2010, 4, 29, 5, 30), ZoneOffset.UTC);
        final SamplePerson person = new SamplePerson("Olivia", birthDate);

        final ByteBuf serialized = serializer.serializeResponseAsBinary(
                ResponseMessage.build().result(Collections.singletonList(person)).code(HttpResponseStatus.OK).create(), allocator);

        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(serialized);

        final SamplePerson actual = (SamplePerson) deserialized.getResult().getData().get(0);
        assertThat(actual, reflectionEquals(person));
    }

    public static class CustomIoRegistry extends AbstractIoRegistry {
        private static final CustomIoRegistry ioreg = new CustomIoRegistry();

        private CustomIoRegistry() {
            register(GraphBinaryIo.class, SamplePerson.class, new SamplePersonSerializer());
        }

        public static CustomIoRegistry instance() {
            return ioreg;
        }
    }
}
