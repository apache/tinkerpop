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

import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.ser.NettyBufferFactory;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;

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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class GraphBinaryReaderWriterRoundTripTest {
    private final GraphBinaryWriter writer = new GraphBinaryWriter();
    private final GraphBinaryReader reader = new GraphBinaryReader();
    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private static NettyBufferFactory bufferFactory = new NettyBufferFactory();

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

        final Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
        nestedMap.put("first", map);

        final List<Object> list = new ArrayList<>();
        list.add("string 1");
        list.add("string 1");
        list.add(200);
        list.add("string 2");

        final List<List<Object>> nestedList = new ArrayList<>();
        nestedList.add(list);

        final Set<Object> set = new HashSet<>();
        set.add("one");
        set.add(2);

        final Set<Set<Object>> nestedSet = new HashSet<>();
        nestedSet.add(set);

        final BulkSet<String> bulkSet = new BulkSet<>();
        bulkSet.add("marko", 1);
        bulkSet.add("josh", 3);

        final Tree<Vertex> tree = new Tree<>();
        final Tree<Vertex> subTree = new Tree<>();
        final Tree<Vertex> subSubTree = new Tree<>();
        subSubTree.put(new ReferenceVertex(1, "animal"), new Tree<>());
        subSubTree.put(new ReferenceVertex(2, "animal"), new Tree<>());
        subTree.put(new ReferenceVertex(100, "animal"), subSubTree);
        tree.put(new ReferenceVertex(1000, "animal"), subTree);

        final MutableMetrics metrics = new MutableMetrics("id1", "name1");
        metrics.setDuration(123, TimeUnit.MICROSECONDS);
        metrics.setCount("c1", 20);
        metrics.setAnnotation("a", "b");
        metrics.addNested(new MutableMetrics("idNested", "nameNested"));

        // can't use the existing 'metrics' because traversal metrics modifies its nested metrics
        final MutableMetrics metrics1 = metrics.clone();

        final MutableMetrics metrics2 = new MutableMetrics("id2", "name2");
        metrics2.setDuration(456, TimeUnit.MICROSECONDS);
        metrics2.setCount("c2", 40);
        metrics2.setAnnotation("c", "d");
        metrics2.addNested(new MutableMetrics("idNested2", "nameNested2"));

        List<MutableMetrics> nestedMetrics = Arrays.asList(metrics1, metrics2);
        final DefaultTraversalMetrics traversalMetrics = new DefaultTraversalMetrics(666, nestedMetrics);
        final DefaultTraversalMetrics emptyTraversalMetrics = new DefaultTraversalMetrics(444, Collections.emptyList());

        return Arrays.asList(
                new Object[] {"String", "ABC", null},
                new Object[] {"Char", '£', null},

                // numerics
                new Object[] {"Byte", 1, null},
                new Object[] {"Integer", 1, null},
                new Object[] {"Float", 2f, null},
                new Object[] {"Double", 3.1d, null},
                new Object[] {"Double", Double.NaN, null},
                new Object[] {"Double", Double.POSITIVE_INFINITY, null},
                new Object[] {"Double", Double.NEGATIVE_INFINITY, null},
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
                new Object[] {"Date", DateFormat.getDateInstance(DateFormat.MEDIUM, Locale.US).parse("Jan 12, 1952"), null},
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
                new Object[] {"ByteBuffer", ByteBuffer.wrap(new byte[]{ 1, 2, 3 }), null},
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
                new Object[] {"Pinside", P.inside(0.0d, 0.6d), null},
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
                new Object[] {"TraversalStrategyVertexProgram", new VertexProgramStrategy(Computer.compute()), (Consumer<TraversalStrategyProxy>) strategy -> {
                    assertEquals(VertexProgramStrategy.class, strategy.getStrategyClass());
                    assertEquals("org.apache.tinkerpop.gremlin.process.computer.GraphComputer", strategy.getConfiguration().getProperty(VertexProgramStrategy.GRAPH_COMPUTER));
                }},
                new Object[] {"TraversalStrategySubgraph", SubgraphStrategy.build().vertices(hasLabel("person")).create(), (Consumer<TraversalStrategyProxy>) strategy -> {
                    assertEquals(SubgraphStrategy.class, strategy.getStrategyClass());
                    assertEquals(hasLabel("person").asAdmin().getBytecode(), strategy.getConfiguration().getProperty(SubgraphStrategy.VERTICES));
                }},
                new Object[] {"BulkSet", bulkSet, null},
                new Object[] {"Tree", tree, null},
                new Object[] {"EmptyMetrics", new MutableMetrics("idEmpty", "nameEmpty"), (Consumer<Metrics>) m -> {
                    assertThat(m, new ReflectionEquals(new MutableMetrics("idEmpty", "nameEmpty")));
                }},
                new Object[] {"Metrics", metrics, (Consumer<Metrics>) m -> {
                    Assert.assertTrue(new ReflectionEquals(metrics, "nested", "counts").matches(m));
                    assertEquals(new ArrayList(metrics.getCounts().values()), new ArrayList(m.getCounts().values()));
                    assertThat(m, new ReflectionEquals(metrics.getNested()));
                }},
                new Object[] {"EmptyTraversalMetrics", emptyTraversalMetrics, (Consumer<TraversalMetrics>) m -> {
                    assertThat(m, new ReflectionEquals(emptyTraversalMetrics));
                }},
                new Object[] {"TraversalMetrics", traversalMetrics, (Consumer<TraversalMetrics>) m -> {
                    assertEquals(m.toString(), traversalMetrics.toString());
                    assertThat(m, new ReflectionEquals(traversalMetrics, "stepIndexedMetrics", "positionIndexedMetrics"));
                }},

                // collections
                new Object[] {"ListSingle", list, null},
                new Object[] {"ListNested", nestedList, null},
                new Object[] {"Map", map, null},
                new Object[] {"Map", nestedMap, null},
                new Object[] {"Set", set, null},
                new Object[] {"SetNested", nestedSet, null});
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
            final Buffer buffer = bufferFactory.create(allocator.buffer());
            writer.write(value, buffer);
            buffer.readerIndex(0);
            final Object result = reader.read(buffer);

            Optional.ofNullable(assertion).orElse((Consumer) r -> assertEquals(value, r)).accept(result);
        }
    }
}
