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
package org.apache.tinkerpop.gremlin.structure.io.graphbinary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledDirectByteBuf;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.AbstractTypedCompatibilityTest;
import org.apache.tinkerpop.gremlin.structure.io.Compatibility;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoCompatibility;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV2d0;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class GraphBinaryCompatibilityTest extends AbstractTypedCompatibilityTest {

    private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private static final GraphBinaryWriter writerV1 = new GraphBinaryWriter();
    private static final GraphBinaryReader readerV1 = new GraphBinaryReader();

    @Parameterized.Parameters(name = "expect({0})")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {GraphBinaryCompatibility.V1_3_4_3, readerV1, writerV1 }
        });
    }

    @Parameterized.Parameter(value = 0)
    public Compatibility compatibility;

    @Parameterized.Parameter(value = 1)
    public GraphBinaryReader reader;

    @Parameterized.Parameter(value = 2)
    public GraphBinaryWriter writer;

    @Override
    public <T> T read(final byte[] bytes, final Class<T> clazz) throws Exception {
        final ByteBuf buffer = allocator.buffer();
        buffer.writeBytes(bytes);
        return reader.read(buffer);
    }

    @Override
    public byte[] write(final Object o, final Class<?> clazz) throws Exception  {
        final ByteBuf buffer = allocator.buffer();
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            writer.write(o, buffer);
            buffer.readerIndex(0);
            buffer.readBytes(stream, buffer.readableBytes());
            return stream.toByteArray();
        }
    }

    @Override
    public Compatibility getCompatibility() {
        return compatibility;
    }
}
