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
package org.apache.tinkerpop.gremlin.driver.ser.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@RunWith(Parameterized.class)
public class GraphBinaryReaderWriterRoundTripTest {
    private final GraphBinaryWriter writer = new GraphBinaryWriter();
    private final GraphBinaryReader reader = new GraphBinaryReader();
    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private final Object value;

    public GraphBinaryReaderWriterRoundTripTest(Object value) {
        this.value = value;
    }

    @Parameterized.Parameters
    public static Collection input() {
        Bytecode bytecode = new Bytecode();
        bytecode.addStep("V");
        bytecode.addStep("tail", 3);
        bytecode.addSource(TraversalSource.Symbols.withComputer, "myComputer");

        Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);

        List list = new ArrayList<>();
        list.add("string 1");

        Set set = new HashSet<>();
        set.add("one");
        set.add(2);

        return Arrays.asList(
                "ABC",
                1, 2f, 3.1d, 10122L, 0, 0f, Integer.MIN_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                UUID.randomUUID(),
                bytecode,
                // Class
                Bytecode.class,
                list, map, set);
    }

    @Test
    public void shouldWriteAndRead() throws Exception {
        ByteBuf buffer = writer.write(value, allocator);
        buffer.readerIndex(0);
        Object result = reader.read(buffer);
        Assert.assertEquals(value, result);
    }
}
