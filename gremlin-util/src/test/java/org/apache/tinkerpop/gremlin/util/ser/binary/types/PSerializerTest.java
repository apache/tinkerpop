/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.util.ser.binary.types;

import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.process.traversal.CompareType;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * GraphBinary encoding details for {@code P.typeOf(Class)}. Format-agnostic round trips of all three
 * {@code P.typeOf} overloads are rows in {@code AbstractRoundTripTest}, which drives GraphBinary and GraphSON.
 */
public class PSerializerTest {

    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    /**
     * The {@code Class} form decodes to {@code String}, so deliberately not {@code equals} to what was written.
     */
    @Test
    public void shouldWriteTypeOfClassAsSimpleName() throws Exception {
        assertEquals(P.typeOf("Boolean"), roundTrip(P.typeOf(Boolean.class)));
    }

    /**
     * The refusal comes from {@code GlobalTypeCache} on evaluation, not from {@code ClassRegistry} on read.
     */
    @Test
    public void shouldFailOnUnregisteredClassWithGlobalTypeCacheMessage() throws Exception {
        final P<Object> read = roundTrip(P.typeOf(UnregisteredType.class));

        try {
            read.test("anything");
            fail("A class that GlobalTypeCache does not hold must not evaluate");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), containsString("is not a registered type"));
            assertThat(ex.getMessage(), containsString(UnregisteredType.class.getSimpleName()));
            assertThat(ex.getMessage(), not(containsString("Class not recognized")));
        }
    }

    private P<Object> roundTrip(final P<Object> p) throws IOException {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        new GraphBinaryWriter().write(p, buffer);

        return (P<Object>) new GraphBinaryReader().read(buffer);
    }

    /**
     * A class {@link CompareType.GlobalTypeCache} does not hold. Never register it.
     */
    private static final class UnregisteredType {
    }
}
