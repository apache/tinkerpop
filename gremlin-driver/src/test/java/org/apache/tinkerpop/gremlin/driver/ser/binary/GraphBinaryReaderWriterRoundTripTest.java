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
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
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

    @Parameterized.Parameters(name = "Type{0}")
    public static Collection input() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("V");
        bytecode.addStep("tail", 3);
        bytecode.addSource(TraversalSource.Symbols.withComputer, "myComputer");

        final Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);

        final List list = new ArrayList<>();
        list.add("string 1");

        final Set set = new HashSet<>();
        set.add("one");
        set.add(2);

        return Arrays.asList(
                new Object[] {"String", "ABC"},

                // numerics
                new Object[] {"Integer", 1},
                new Object[] {"Float", 2f},
                new Object[] {"Double", 3.1d},
                new Object[] {"Long", 10122L},
                new Object[] {"IntegerZero", 0},
                new Object[] {"FloatZero", 0f},
                new Object[] {"IntegerMin", Integer.MIN_VALUE},
                new Object[] {"IntegerMax", Integer.MAX_VALUE},
                new Object[] {"LongMax", Long.MAX_VALUE},

                new Object[] {"UUID", UUID.randomUUID()},
                new Object[] {"Bytecode", bytecode},
                new Object[] {"Class", Bytecode.class},

                // enums
                new Object[] {"Barrier", SackFunctions.Barrier.normSack},
                new Object[] {"Cardinality", VertexProperty.Cardinality.list},
                new Object[] {"Columns", Column.values},
                new Object[] {"Direction", Direction.BOTH},
                new Object[] {"Operator", Operator.sum},
                new Object[] {"Order", Order.desc},
                new Object[] {"Pick", TraversalOptionParent.Pick.any},
                new Object[] {"Pop", Pop.mixed},
                new Object[] {"Scope", Scope.global},
                new Object[] {"T", T.label},

                // collections
                new Object[] {"List", list},
                new Object[] {"Map", map},
                new Object[] {"Set", set});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Object value;

    @Test
    public void shouldWriteAndRead() throws Exception {
        final ByteBuf buffer = writer.write(value, allocator);
        buffer.readerIndex(0);
        final Object result = reader.read(buffer);
        Assert.assertEquals(value, result);
    }
}
