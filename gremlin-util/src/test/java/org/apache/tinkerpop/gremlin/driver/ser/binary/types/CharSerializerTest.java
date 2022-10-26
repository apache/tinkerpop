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
package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.ser.NettyBufferFactory;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CharSerializerTest {
    private final ByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    private static final GraphBinaryReader reader = new GraphBinaryReader();
    private static final GraphBinaryWriter writer = new GraphBinaryWriter();

    @Parameterized.Parameters(name = "Character={0}")
    public static Collection input() {
        return Arrays.asList(
                new Object[] {'a', new byte[]{ 0x61 }},
                new Object[] {'b', new byte[]{ 0x62 }},
                new Object[] {'$', new byte[]{ 0x24 }},
                new Object[] {'¢', new byte[]{ (byte)0xc2, (byte)0xa2 }},
                new Object[] {'€', new byte[]{ (byte)0xe2, (byte)0x82, (byte)0xac }},
                new Object[] {'ह', new byte[]{ (byte)0xe0, (byte)0xa4, (byte)0xb9 }});
    }

    @Parameterized.Parameter(value = 0)
    public char charValue;

    @Parameterized.Parameter(value = 1)
    public byte[] byteArray;

    @Test
    public void readValueTest() throws IOException {
        final Character actual = reader.readValue(
                bufferFactory.create(Unpooled.wrappedBuffer(byteArray)),Character.class, false);
        assertEquals(charValue, actual.charValue());
    }

    @Test
    public void writeValueTest() throws IOException {
        final Buffer actual = bufferFactory.create(allocator.buffer());
        writer.writeValue(charValue, actual, false);
        final byte[] actualBytes = new byte[byteArray.length];
        actual.readBytes(actualBytes);
        assertTrue(Arrays.deepEquals(new byte[][]{byteArray}, new byte[][]{actualBytes}));
    }
}
