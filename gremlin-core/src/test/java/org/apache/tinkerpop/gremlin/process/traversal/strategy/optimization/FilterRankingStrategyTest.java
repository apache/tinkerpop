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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.filter;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.not;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.or;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.properties;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class FilterRankingStrategyTest {

    public static Iterable<Object[]> data() {
        return generateTestParameters();
    }

    @Parameterized.Parameter(value = 0)
    public Traversal.Admin original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    @Parameterized.Parameter(value = 2)
    public Collection<TraversalStrategy> otherStrategies;

    @Test
    public void doTest() {
        final String repr = original.getGremlinLang().getGremlin("__");
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(FilterRankingStrategy.instance());
        for (final TraversalStrategy strategy : this.otherStrategies) {
            strategies.addStrategies(strategy);
        }
        this.original.asAdmin().setStrategies(strategies);
        this.original.asAdmin().applyStrategies();
        assertEquals(repr, this.optimized, this.original);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        final Predicate testP = t -> true;

        return Arrays.asList(new Object[][]{
                {__.has("name", "marko").as("a").out().as("b").has("age", 32).where("a", neq("b")).as("c").out(),
                        __.has("name", "marko").as("a").out().has("age", 32).as("b").where("a", neq("b")).as("c").out(),
                        Collections.emptyList()},

                {__.V().as("n").where(__.or(__.select("n").hasLabel("software"),__.select("n").hasLabel("person"))).select("n").by("name"),
                        __.V().as("n").where(__.or(__.select("n").hasLabel("software"),__.select("n").hasLabel("person"))).select("n").by("name"),
                        Collections.emptyList()},

                {__.V().as("n").where(__.where(__.where(__.where(__.or(__.select("n").hasLabel("software"),__.select("n").hasLabel("person")))))).select("n").by("name"),
                        __.V().as("n").where(__.where(__.where(__.where(__.or(__.select("n").hasLabel("software"),__.select("n").hasLabel("person")))))).select("n").by("name"),
                        Collections.emptyList()},

                {__.V().as("n").where(__.where(__.where(__.where(__.or(__.select("n").hasLabel("software"),__.select("n").hasLabel("person")))))).has("test", "val").select("n").by("name"),
                        __.V().has("test", "val").as("n").where(__.where(__.where(__.where(__.or(__.select("n").hasLabel("software"),__.select("n").hasLabel("person")))))).select("n").by("name"),
                        Collections.emptyList()},

                {__.V().as("n").out().where(__.or(__.hasLabel("out-dest-label1").select("n").hasLabel("software"),__.hasLabel("out-dest-label2").select("n").hasLabel("person"))).select("n").by("name"),
                        __.V().as("n").out().where(__.or(__.hasLabel("out-dest-label1").select("n").hasLabel("software"),__.hasLabel("out-dest-label2").select("n").hasLabel("person"))).select("n").by("name"),
                        Collections.emptyList()},

                // some lambda tests, where reordering over lambdas should not happen
                {__.order().map(Lambda.function("it.get().out()")).dedup(),
                        __.order().map(Lambda.function("it.get().out()")).dedup(),
                        Collections.emptyList()},

                  {__.order().filter(__.map(Lambda.function("it.get().out()"))).dedup(),
                        __.order().filter(__.map(Lambda.function("it.get().out()"))).dedup(),
                        Collections.emptyList()},

                {__.dedup().order(),
                        __.dedup().order(),
                        Collections.emptyList()},
                {__.has("name", "marko").as("a").out().as("b").has("age", 32).where("a", neq("b")).as("c").out(),
                        __.has("name", "marko").as("a").out().has("age", 32).as("b").where("a", neq("b")).as("c").out(),
                        Collections.emptyList()},
                {__.has("name", "marko").as("a").out().has("age", 32).as("b").where("a", neq("b")),
                        __.has("name", "marko").as("a").out().has("age", 32).as("b").where("a", neq("b")),
                        Collections.emptyList()},
                {__.has("name", "marko").has("age", 32).dedup().has("name", "bob").as("a"),
                        __.has("name", "marko").has("age", 32).has("name", "bob").dedup().as("a"),
                        Collections.singletonList(InlineFilterStrategy.instance())},
                {__.has("name", "marko").dedup().as("a").has("age", 32).has("name", "bob").as("b"),
                        __.has("name", "marko").has("age", 32).has("name", "bob").dedup().as("b", "a"),
                        Collections.singletonList(InlineFilterStrategy.instance())},
                {__.where("b", eq("c")).as("a").dedup("a").has("name", "marko"),
                        __.has("name", "marko").where("b", eq("c")).as("a").dedup("a"),
                        Collections.emptyList()},
                {__.where("b", eq("c")).has("name", "bob").as("a").dedup("a").has("name", "marko"),
                        __.has("name", "bob").has("name", "marko").where("b", eq("c")).as("a").dedup("a"),
                        Collections.singletonList(InlineFilterStrategy.instance())},
                {__.has("name", "marko").as("a").out().has("name", "bob").dedup().as("b").where(__.as("a").out().as("b")),
                        __.has("name", "marko").as("a").out().has("name", "bob").dedup().as("b").where(__.as("a").out().as("b")),
                        Collections.singletonList(InlineFilterStrategy.instance())},
                {__.has("name", "marko").as("a").out().has("name", "bob").as("b").dedup().where(__.as("a").out().as("b")),
                        __.has("name", "marko").as("a").out().has("name", "bob").dedup().as("b").where(__.as("a").out().as("b")),
                        Collections.singletonList(InlineFilterStrategy.instance())},
                {__.has("name", "marko").as("a").out().has("name", "bob").dedup().as("c").where(__.as("a").out().as("b")),
                        __.has("name", "marko").as("a").out().has("name", "bob").where(__.as("a").out().as("b")).dedup().as("c"),
                        Collections.singletonList(InlineFilterStrategy.instance())},
                {__.order().dedup(),
                        __.dedup().order(),
                        Collections.emptyList()},
                {__.order().filter(testP).dedup(),
                        __.order().filter(testP).dedup(),
                        Collections.emptyList()},
                {__.order().as("a").dedup(),
                        __.dedup().order().as("a"),
                        Collections.emptyList()},
                {__.order().as("a").dedup("a"),
                        __.order().as("a").dedup("a"),
                        Collections.emptyList()},
                {__.order().as("a").dedup("a").has("name", "marko"),
                        __.has("name", "marko").as("a").dedup("a").order(),
                        Collections.emptyList()},
                {__.order().as("a").dedup("a").has("name", "marko").out(),
                        __.has("name", "marko").as("a").dedup("a").order().out(),
                        Collections.emptyList()},
                {__.order().as("a").dedup("a").has("name", "marko").where("a", eq("b")).out(),
                        __.has("name", "marko").as("a").where("a", eq("b")).dedup("a").order().out(),
                        Collections.emptyList()},
                {__.identity().order().dedup(),
                        __.dedup().order(),
                        Collections.singletonList(IdentityRemovalStrategy.instance())},
                {__.order().identity().dedup(),
                        __.dedup().order(),
                        Collections.singletonList(IdentityRemovalStrategy.instance())},
                {__.order().out().dedup(),
                        __.order().out().dedup(),
                        Collections.emptyList()},
                {has("value", 0).filter(out()).dedup(),
                        has("value", 0).filter(out()).dedup(),
                        Collections.emptyList()},
                {__.dedup().has("value", 0).or(not(has("age")), has("age", 10)).has("value", 1),
                        __.has("value", 0).has("value", 1).or(not(has("age")), has("age", 10)).dedup(),
                        Collections.singletonList(InlineFilterStrategy.instance())},
                {__.dedup().filter(out()).has("value", 0),
                        has("value", 0).filter(out()).dedup(),
                        Collections.emptyList()},
                {filter(out()).dedup().has("value", 0),
                        has("value", 0).filter(out()).dedup(),
                        Collections.emptyList()},
                {__.as("a").out().has("age").where(P.eq("a")),
                        __.as("a").out().where(P.eq("a")).has("age"),
                        Collections.emptyList()},
                {__.as("a").out().has("age").where(P.eq("a")).by("age"),
                        __.as("a").out().has("age").where(P.eq("a")).by("age"),
                        Collections.emptyList()},
                {__.as("a").out().and(has("age"), has("name")).where(P.eq("a")).by("age"),
                        __.as("a").out().and(has("age"), has("name")).where(P.eq("a")).by("age"),
                        Collections.emptyList()},
                {__.as("a").out().and(has("age"), has("name")).where(P.eq("a")),
                        __.as("a").out().where(P.eq("a")).and(has("age"), has("name")),
                        Collections.emptyList()},
                {__.as("a").out().and(has("age"), has("name")).where(P.eq("a")).by("age"),
                        __.as("a").out().has("age").has("name").where(P.eq("a")).by("age"),
                        Collections.singletonList(InlineFilterStrategy.instance())},
                {__.as("a").out().and(has("age"), has("name")).where(P.eq("a")),
                        __.as("a").out().where(P.eq("a")).has("age").has("name"),
                        Collections.singletonList(InlineFilterStrategy.instance())},
                {__.as("a").out().and(has("age"), has("name")).filter(__.where(P.eq("a"))),
                        __.as("a").out().where(P.eq("a")).has("age").has("name"),
                        Collections.singletonList(InlineFilterStrategy.instance())},
                {__.as("a").out().and(has("age"), has("name")).filter(__.where(P.eq("a")).by("age")),
                        __.as("a").out().has("age").has("name").where(P.eq("a")).by("age"),
                        Collections.singletonList(InlineFilterStrategy.instance())},
                {has("value", 0).filter(out()).dedup(),
                        has("value", 0).filter(out()).dedup(),
                        Collections.emptyList()},
                {has("value", 0).or(has("name"), has("age")).has("value", 1).dedup(),
                        has("value", 0).has("value", 1).or(has("name"), has("age")).dedup(),
                        Collections.singletonList(InlineFilterStrategy.instance())},
                {has("value", 0).or(out(), in()).as(Graph.Hidden.hide("x")).has("value", 1).dedup(),
                        has("value", 0).has("value", 1).or(outE(), inE()).dedup(),
                        TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList()},
                {has("value", 0).and(has("age"), has("name", "marko")).is(10),
                        __.is(10).has("value", 0).has("age").has("name", "marko"),
                        Collections.singletonList(InlineFilterStrategy.instance())},
                {has("value", 0).filter(or(not(has("age")), has("age", 1))).has("value", 1).dedup(),
                        has("value", 0).has("value", 1).or(not(filter(properties("age"))), has("age", 1)).dedup(),
                        TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList()},
        });
    }
}


