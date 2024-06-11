/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.DatetimeHelper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.util.CollectionUtil.asMap;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class GremlincodeTest {

    private static final GraphTraversalSource g = traversal().with(EmptyGraph.instance());

    @Parameterized.Parameter(value = 0)
    public Traversal traversal;

    @Parameterized.Parameter(value = 1)
    public String expected;

    @Test
    public void doTest() {
        final String gremlin = traversal.asAdmin().getGremlinLang().getGremlin();
        assertEquals(expected, gremlin);
    }

    @Before
    public void setUp() {

    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        return Arrays.asList(new Object[][]{
                {g.V().count(), "g.V().count()"},
                {g.addV("test"), "g.addV(\"test\")"},
                {g.addV("t\"'est"), "g.addV(\"t\\\"'est\")"},
                {g.inject(true, (byte) 1, (short) 2, 3, 4L, 5f, 6d), "g.inject(true,1B,2S,3,4L,5.0F,6.0D)"},
                {g.inject(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY),
                        "g.inject(+Infinity,-Infinity)"},
                {g.inject(DatetimeHelper.parse("2018-03-21T08:35:44.741Z")),
                        "g.inject(datetime(\"2018-03-21T08:35:44.741Z\"))"},
                {g.inject(asMap("age", VertexProperty.Cardinality.list(33))),
                        "g.inject([\"age\":Cardinality.list(33)])"},
                {g.V(1).out("knows").values("name"), "g.V(1).out(\"knows\").values(\"name\")"},
                {g.V().has(T.label, "person"), "g.V().has(T.label,\"person\")"},
                {g.addE("knows").from(new DetachedVertex(1, "test1", Collections.emptyList())).to(new DetachedVertex(6, "test2", Collections.emptyList())),
                        "g.addE(\"knows\").from(new ReferenceVertex(1,\"test1\")).to(new ReferenceVertex(6,\"test2\"))"},
                {g.V(Bindings.instance().of("x", 1)), "g.V(1)"},
                {g.V().hasId(P.within(Collections.emptyList())).count(), "g.V().hasId(P.within(_0)).count()"},
                {g.V(1).outE().has("weight", P.inside(0.0, 0.6)), "g.V(1).outE().has(\"weight\",P.gt(0.0D).and(P.lt(0.6D)))"},
                {g.withSack(1.0, Operator.sum).V(1).local(__.out("knows").barrier(SackFunctions.Barrier.normSack)).in("knows").barrier().sack(),
                        "g.withSack(1.0D,Operator.sum).V(1).local(__.out(\"knows\").barrier(Barrier.normSack)).in(\"knows\").barrier().sack()"},
                {g.inject(Arrays.asList(1, 2, 3)).skip(local, 1), "g.inject(_1).skip(Scope.local,1L)"},
                {g.V().has("name", "marko").
                        project("name", "friendsNames").
                        by("name").
                        by(out("knows").values("name").fold()),
                        "g.V().has(\"name\",\"marko\")." +
                                "project(\"name\",\"friendsNames\")." +
                                "by(\"name\")." +
                                "by(__.out(\"knows\").values(\"name\").fold())"},
                {g.inject(Arrays.asList(5, 6)).union(__.V(Arrays.asList(1, 2)), __.V(Arrays.asList(3, 4))),
                        "g.inject(_2).union(__.V(_3),__.V(_4))"},
                {g.with("evaluationTimeout", 1000).V(), "g.V()"},
                {g.withSideEffect("a", 1).V(), "g.withSideEffect(\"a\",1).V()"},
                {g.withStrategies(ReadOnlyStrategy.instance()).V(), "g.withStrategies(ReadOnlyStrategy).V()"},
                {g.withStrategies(new SeedStrategy(999999)).V().order().by("name").coin(0.5),
                        "g.withStrategies(new SeedStrategy(seed:999999L)).V().order().by(\"name\").coin(0.5D)"},
                {g.withStrategies(SubgraphStrategy.build().vertices(hasLabel("person")).create()).V(),
                        "g.withStrategies(new SubgraphStrategy(checkAdjacentVertices:true,vertices:__.hasLabel(\"person\"))).V()",},
                {g.withStrategies(SubgraphStrategy.build().vertices(__.has("name", P.within("josh", "lop", "ripple"))).create()).V(),
                        "g.withStrategies(new SubgraphStrategy(checkAdjacentVertices:true,vertices:__.has(\"name\",P.within(_5)))).V()"},
        });
    }
}
