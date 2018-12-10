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
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Consumer;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class GraphBinaryReaderWriterRoundTripTest {
    private final GraphBinaryWriter writer = new GraphBinaryWriter();
    private final GraphBinaryReader reader = new GraphBinaryReader();
    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    private static final GraphTraversalSource g = TinkerFactory.createModern().traversal();

    @Parameterized.Parameters(name = "Type{0}")
    public static Collection input() throws Exception {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("V");
        bytecode.addStep("tail", 3);
        bytecode.addSource(TraversalSource.Symbols.withComputer, "myComputer");

        final Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);

        final List listSingle = new ArrayList<>();
        listSingle.add("string 1");

        final List listMulti = new ArrayList<>();
        listSingle.add("string 1");
        listSingle.add(200);
        listSingle.add("string 2");

        final Set set = new HashSet<>();
        set.add("one");
        set.add(2);

        return Arrays.asList(
                new Object[] {"String", "ABC", null},
                new Object[] {"Char", '£', null},

                // numerics
                new Object[] {"Byte", 1, null},
                new Object[] {"Integer", 1, null},
                new Object[] {"Float", 2f, null},
                new Object[] {"Double", 3.1d, null},
                new Object[] {"Long", 10122L, null},
                new Object[] {"IntegerZero", 0, null},
                new Object[] {"FloatZero", 0f, null},
                new Object[] {"IntegerMin", Integer.MIN_VALUE, null},
                new Object[] {"IntegerMax", Integer.MAX_VALUE, null},
                new Object[] {"LongMax", Long.MAX_VALUE, null},
                new Object[] {"BigIntegerPos", new BigInteger("1234567890987654321"), null},
                new Object[] {"BigIntegerNeg", new BigInteger("-1234567890987654321"), null},
                new Object[] {"BigDecimalPos", new BigDecimal("1234567890987654321.1232132"), null},
                new Object[] {"BigDecimalNeg", new BigDecimal("-1234567890987654321.1232132"), null},

                // date+time
                new Object[] {"Date", DateFormat.getDateInstance(DateFormat.MEDIUM).parse("Jan 12, 1952"), null},
                new Object[] {"Timestamp", Timestamp.valueOf("2016-01-15 12:01:02"), null},
                new Object[] {"Duration", Duration.ofSeconds(213123213, 400), null},
                new Object[] {"Instant", Instant.ofEpochSecond(213123213, 400), null},
                new Object[] {"LocalDate", LocalDate.of(2016, 10, 21), null},
                new Object[] {"LocalTime", LocalTime.of(12, 20, 30, 300), null},
                new Object[] {"LocalDateTime", LocalDateTime.of(2016, 10, 21, 12, 20, 30, 300), null},
                new Object[] {"MonthDay", MonthDay.of(12, 28), null},
                new Object[] {"OffsetDateTime", OffsetDateTime.of(2017, 11, 15, 12, 30, 45, 300, ZoneOffset.ofTotalSeconds(400)), null},
                new Object[] {"OffsetTime", OffsetTime.of(12, 30, 45, 300, ZoneOffset.ofTotalSeconds(400)), null},
                new Object[] {"Period", Period.of(1, 6, 15), null},
                new Object[] {"Year", Year.of(1996), null},
                new Object[] {"YearMonth", YearMonth.of(2016, 11), null},
                new Object[] {"ZonedDateTime", ZonedDateTime.of(2016, 11, 15, 12, 30, 45, 300, ZoneOffset.ofTotalSeconds(200)), null},
                new Object[] {"ZoneOffset", ZoneOffset.ofTotalSeconds(100), null},

                new Object[] {"UUID", UUID.randomUUID(), null},
                new Object[] {"Bytecode", bytecode, null},
                new Object[] {"Binding", new Bytecode.Binding<>("x", 123), null},
                new Object[] {"Traverser", new DefaultRemoteTraverser<>("marko", 100), null},
                new Object[] {"Class", Bytecode.class, null},
                new Object[] {"ByteBuffer", ByteBuffer.allocate(8).putLong(123456), null},
                new Object[] {"InetAddressV4", InetAddress.getByName("localhost"), null},
                new Object[] {"InetAddressV6", InetAddress.getByName("::1"), null},
                new Object[] {"Lambda0", Lambda.supplier("return 1"), null},
                new Object[] {"Lambda1", Lambda.consumer("it"), null},
                new Object[] {"Lambda2", Lambda.biFunction("x,y -> x + y"), null},
                new Object[] {"LambdaN", new Lambda.UnknownArgLambda("x,y,z -> x + y + z", "gremlin-groovy", 3), null},

                // enums
                new Object[] {"Barrier", SackFunctions.Barrier.normSack, null},
                new Object[] {"Cardinality", VertexProperty.Cardinality.list, null},
                new Object[] {"Columns", Column.values, null},
                new Object[] {"Direction", Direction.BOTH, null},
                new Object[] {"Operator", Operator.sum, null},
                new Object[] {"Operator", Operator.div, null},
                new Object[] {"Order", Order.desc, null},
                new Object[] {"Pick", TraversalOptionParent.Pick.any, null},
                new Object[] {"Pop", Pop.mixed, null},
                new Object[] {"Scope", Scope.global, null},
                new Object[] {"T", T.label, null},
                new Object[] {"Pgt", P.gt(0), null},
                new Object[] {"Pgte", P.gte(0), null},
                new Object[] {"Pbetween", P.between(0,1), null},
                new Object[] {"Pand", P.gt(1).and(P.lt(2)), null},
                new Object[] {"Por", P.gt(1).or(P.lt(2)), null},
                new Object[] {"Pnot", P.not(P.lte(1)), null},
                new Object[] {"Pwithout", P.without(1,2,3,4), null},
                new Object[] {"TextP", TextP.startingWith("mark"), null},

                // graph
                new Object[] {"ReferenceEdge", new ReferenceEdge(123, "person", new ReferenceVertex(123, "person"), new ReferenceVertex(321, "person")), null},
                new Object[] {"TinkerEdge", g.E().hasLabel("knows").next(), null},
                new Object[] {"ReferenceProperty", new ReferenceProperty<>("name", "john"), (Consumer<ReferenceProperty>) referenceProperty -> {
                    assertEquals("name", referenceProperty.key());
                    assertEquals("john", referenceProperty.value());
                }},
                new Object[] {"ReferenceVertex", new ReferenceVertex(123, "person"), null},
                new Object[] {"TinkerVertex", g.V().hasLabel("person").next(), null},
                new Object[] {"ReferenceVertexProperty", new ReferenceVertexProperty<>(123, "name", "john"), (Consumer<ReferenceVertexProperty>) referenceProperty -> {
                    assertEquals("name", referenceProperty.key());
                    assertEquals("john", referenceProperty.value());
                    assertEquals(123, referenceProperty.id());
                }},
                new Object[] {"PathLabelled", g.V().as("a", "b").out().as("c").path().next(), null},
                new Object[] {"PathNotLabelled", g.V().out().inE().values().path().next(), null},
                new Object[] {"Graph", g.E().subgraph("k").cap("k").next(), (Consumer<Graph>) graph -> {
                    IoTest.assertModernGraph(graph, true, false);
                }},
                new Object[] {"TraversalStrategy", SubgraphStrategy.build().vertices(hasLabel("person")).create(), (Consumer<TraversalStrategyProxy>) strategy -> {
                    assertEquals(SubgraphStrategy.class, strategy.getStrategyClass());
                    assertEquals(hasLabel("person").asAdmin().getBytecode(), strategy.getConfiguration().getProperty(SubgraphStrategy.VERTICES));
                }},

                // collections
                new Object[] {"ListSingle", listSingle, null},
                new Object[] {"ListMulti", listMulti, null},
                new Object[] {"Map", map, null},
                new Object[] {"Set", set, null});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Object value;

    @Parameterized.Parameter(value = 2)
    public Consumer<?> assertion;

    @Test
    public void shouldWriteAndRead() throws Exception {
        // Test it multiple times as the type registry might change its internal state
        for (int i = 0; i < 5; i++) {
            final ByteBuf buffer = writer.write(value, allocator);
            buffer.readerIndex(0);
            final Object result = reader.read(buffer);

            Optional.ofNullable(assertion).orElse((Consumer) r -> assertEquals(value, r)).accept(result);
        }
    }
}
