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
package org.apache.tinkerpop.gremlin.util.ser;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public abstract class AbstractRoundTripTest {

    protected static final GraphTraversalSource g = TinkerFactory.createModern().traversal();
    @Parameterized.Parameters(name = "Type{0}")
    public static Collection input() throws Exception {
        final Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);

        final Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
        nestedMap.put("first", map);

        final Map<String, Integer> orderedMap = new LinkedHashMap<>();
        map.put("one", 1);
        map.put("two", 2);

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
                // todo: add tests for non-ASCII chars
                new Object[] {"Char", '$', null},

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
                new Object[] {"Duration", Duration.ofSeconds(213123213, 400), null},

                new Object[] {"UUID", UUID.randomUUID(), null},
                new Object[] {"ByteBuffer", ByteBuffer.wrap(new byte[]{ 1, 2, 3 }), null},

                // enums
                new Object[] {"Direction", Direction.BOTH, null},
                new Object[] {"T", T.label, null},

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
                new Object[] {"BulkList", bulkSet, (Consumer<List>) bulklist -> {
                    List bulkSetExpanded = new ArrayList<>();
                    bulkSet.spliterator().forEachRemaining(bulkSetExpanded::add);
                    assertEquals(bulkSetExpanded, bulklist);
                }},
                new Object[] {"Tree", tree, null},

                // collections
                new Object[] {"ListSingle", list, null},
                new Object[] {"ListNested", nestedList, null},
                new Object[] {"Map", map, null},
                new Object[] {"MapNested", nestedMap, null},
                new Object[] {"OrderedMap", orderedMap, null},
                new Object[] {"Set", set, null},
                new Object[] {"SetNested", nestedSet, null});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Object value;

    @Parameterized.Parameter(value = 2)
    public Consumer<?> assertion;
}
