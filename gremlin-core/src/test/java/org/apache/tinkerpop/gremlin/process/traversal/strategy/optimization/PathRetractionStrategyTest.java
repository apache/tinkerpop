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

import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.util.collections.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gte;
import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.without;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.limit;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.project;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.store;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.where;
import static org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy.MAX_BARRIER_SIZE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Ted Wilmes (http://twilmes.org)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class PathRetractionStrategyTest {

    private final List<TraversalStrategies> strategies = Arrays.asList(
            new DefaultTraversalStrategies().addStrategies(PathRetractionStrategy.instance()),
            new DefaultTraversalStrategies().addStrategies(PathRetractionStrategy.instance(), PathProcessorStrategy.instance()),
            new DefaultTraversalStrategies().addStrategies(PathRetractionStrategy.instance(), PathProcessorStrategy.instance(), MatchPredicateStrategy.instance()),
            new DefaultTraversalStrategies().addStrategies(PathRetractionStrategy.instance(), PathProcessorStrategy.instance(), MatchPredicateStrategy.instance(), RepeatUnrollStrategy.instance()));

    @Parameterized.Parameter(value = 0)
    public Traversal.Admin traversal;

    @Parameterized.Parameter(value = 1)
    public String labels;

    @Parameterized.Parameter(value = 2)
    public Traversal.Admin optimized;

    private static final String PATH_RETRACTION_STRATEGY_DISABLED = "[]";

    @Test
    public void doTest() {
        for (final TraversalStrategies currentStrategies : this.strategies) {
            final Traversal.Admin<?, ?> currentTraversal = this.traversal.clone();
            currentTraversal.setStrategies(currentStrategies);
            currentTraversal.applyStrategies();
            assertEquals(this.labels, getKeepLabels(currentTraversal).toString());
            if (null != optimized)
                assertEquals(currentTraversal, optimized);
        }
    }

    private List<Object> getKeepLabels(final Traversal.Admin<?, ?> traversal) {
        List<Object> keepLabels = new ArrayList<>();
        for (Step step : traversal.getSteps()) {
            if (step instanceof PathProcessor) {
                final Set<String> keepers = ((PathProcessor) step).getKeepLabels();
                if (keepers != null)
                    keepLabels.add(keepers);
            }
            if (step instanceof TraversalParent) {
                final TraversalParent parent = (TraversalParent) step;
                final List<Traversal.Admin<?, ?>> children = new ArrayList<>();
                children.addAll(parent.getGlobalChildren());
                children.addAll(parent.getLocalChildren());
                for (final Traversal.Admin<?, ?> child : children) {
                    final List<Object> childLabels = getKeepLabels(child);
                    if (childLabels.size() > 0) {
                        keepLabels.add(childLabels);
                    }
                }
            }
        }
        return keepLabels;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        final VertexProgram labeledPathVertexProgram = mock(VertexProgram.class);
        final VertexProgram pathVertexProgram = mock(VertexProgram.class);
        final VertexProgram emptyRequirementsVertexProgram = mock(VertexProgram.class);

        when(labeledPathVertexProgram.getTraverserRequirements()).thenReturn(Sets.newSet(TraverserRequirement.LABELED_PATH));
        when(pathVertexProgram.getTraverserRequirements()).thenReturn(Sets.newSet(TraverserRequirement.PATH));
        when(emptyRequirementsVertexProgram.getTraverserRequirements()).thenReturn(Collections.EMPTY_SET);

        return Arrays.asList(new Object[][]{
                {out(), "[]", null},
                {__.V().as("a").out().as("b").where(neq("a")).out(), "[[]]", null},
                {__.V().as("a").out().where(out().where(neq("a"))).out(), "[[[]]]", null},
                {__.V().as("a").out().where(neq("a")).out().select("a"), "[[a], []]", null},
                {__.V().as("a").out().as("b").where(neq("a")).out().select("a", "b").out().select("b"), "[[a, b], [b], []]", null},
                {__.V().match(as("a").out().as("b")), "[[a, b]]", null},
                {__.V().match(as("a").out().as("b")).select("a"), "[[a], []]", null},
                {__.V().out().out().match(
                        as("a").in("created").as("b"),
                        as("b").in("knows").as("c")).select("c").out("created").where(neq("a")).values("name"),
                        "[[a, c], [a], []]", null},
                {__.V().as("a").out().select("a").path(), PATH_RETRACTION_STRATEGY_DISABLED, null},
                {__.V().as("a").out().select("a").map(t -> t.path().get("a")), PATH_RETRACTION_STRATEGY_DISABLED, null}, // lambda introspection is not possible
                {__.V().as("a").out().select("a").subgraph("b"), "[[]]", null},
                {__.V().as("a").out().select("a").subgraph("b").select("a"), "[[a], []]", null},
                {__.V().out().out().match(
                        as("a").in("created").as("b"),
                        as("b").in("knows").as("c")).select("c").out("created").where(neq("a")).values("name").path(),
                        PATH_RETRACTION_STRATEGY_DISABLED, null},
                {__.V().out().as("a").where(neq("a")).out().where(neq("a")).out(), "[[a], []]", null},
                {__.V().out().as("a").where(out().select("a").values("prop").count().is(gte(1))).out().where(neq("a")), "[[[a]], []]", null},
                {__.V().as("a").out().as("b").where(out().select("a", "b", "c").values("prop").count().is(gte(1))).out().where(neq("a")).out().select("b"),
                        "[[[a, b]], [b], []]", null},
                {__.outE().inV().group().by(__.inE().outV().groupCount().by(__.both().count().is(P.gt(2)))), "[]", null},
                {__.V().as("a").repeat(out().where(neq("a"))).emit().select("a").values("test"), "[[[a]], []]", null},
                // given the way this test harness is structured, I have to manual test for RepeatUnrollStrategy (and it works) TODO: add more test parameters
                // {__.V().as("a").repeat(__.out().where(neq("a"))).times(3).select("a").values("test"), Arrays.asList(Collections.singleton("a"), Collections.singleton("a"), Collections.singleton("a"), Collections.emptySet())}
                {__.V().as("a").out().as("b").select("a").out().out(), "[[]]", __.V().as("a").out().as("b").select("a").barrier(MAX_BARRIER_SIZE).out().out()},
                {__.V().as("a").out().as("b").select("a").count(), "[[]]", __.V().as("a").out().as("b").select("a").count()},
                {__.V().as("a").out().as("b").select("a").barrier().count(), "[[]]", __.V().as("a").out().as("b").select("a").barrier().count()},
                {__.V().as("a").out().as("b").dedup("a", "b").out(), "[[]]", __.V().as("a").out().as("b").dedup("a", "b").out()},
                {__.V().as("a").out().as("b").match(as("a").out().as("b")), "[[a, b]]", __.V().as("a").out().as("b").match(as("a").out().as("b"))},
                {__.V().as("a").out().as("b").match(as("a").out().as("b")).select("a"), "[[a], []]", __.V().as("a").out().as("b").match(as("a").out().as("b")).select("a")},
                {__.V().as("a").out().as("b").match(as("a").out().as("b")).select("a").out().dedup("a"), "[[a], [a], []]", __.V().as("a").out().as("b").match(as("a").out().as("b")).select("a").barrier(MAX_BARRIER_SIZE).out().dedup("a")},
                {__.V().as("a").out().as("b").where(P.gt("a")).out().out(), "[[]]", __.V().as("a").out().as("b").where(P.gt("a")).barrier(MAX_BARRIER_SIZE).out().out()},
                {__.V().as("a").out().as("b").where(P.gt("a")).count(), "[[]]", __.V().as("a").out().as("b").where(P.gt("a")).count()},
                {__.V().as("a").out().as("b").select("a").as("c").where(P.gt("b")).out(), "[[b], []]", __.V().as("a").out().as("b").select("a").as("c").barrier(MAX_BARRIER_SIZE).where(P.gt("b")).barrier(MAX_BARRIER_SIZE).out()},
                {__.V().select("c").map(select("c").map(select("c"))).select("c"), "[[c], [[c], [[c]]], []]", null},
                {__.V().select("c").map(select("c").map(select("c"))).select("b"), "[[b, c], [[b, c], [[b]]], []]", null},
                {__.V().as("a").out().as("b").select("a").select("b").union(
                        as("c").out().as("d", "e").select("c", "e").out().select("c"),
                        as("c").out().as("d", "e").select("c", "e").out().select("c")).
                        out().select("c"),
                        "[[b, c, e], [c, e], [[c, e], [c, e]], [[c, e], [c, e]], []]", null},
                {__.V().as("a").out().as("b").select("a").select("b").
                        local(as("c").out().as("d", "e").select("c", "e").out().select("c")).
                        out().select("c"),
                        "[[b, c, e], [c, e], [[c], [c]], []]", null},
                // TODO: same as above but note how path() makes things react
//                {__.V().as("a").out().as("b").select("a").select("b").path().local(as("c").out().as("d", "e").select("c", "e").out().select("c")).out().select("c"),
//                        "[[[c, e], [c, e]]]", null},
                {__.V().as("a").out().as("b").select("a").select("b").repeat(out().as("c").select("b", "c").out().select("c")).out().select("c").out().select("b"),
                        "[[b, c], [b, c], [[b, c], [b, c]], [b], []]", null},
                {__.V().as("a").out().as("b").select("a").select("b").repeat(out().as("c").select("b")).out().select("c").out().select("b"),
                        "[[b, c], [b, c], [[b, c]], [b], []]", null},
                {__.V().as("a").out().as("b").select("a").select("b").repeat(out().as("c").select("b")),
                        "[[b], [b], [[b]]]", null},
                {__.V().select("a").map(select("c").map(select("b"))).select("c"),
                        "[[b, c], [[b, c], [[c]]], []]", null},
                {__.V().select("a").map(select("b").repeat(select("c"))).select("a"),
                        "[[a, b, c], [[a, c], [[a, c]]], []]", null},
                {__.V().select("c").map(select("c").map(select("c"))).select("c"), "[[c], [[c], [[c]]], []]", null},
                {__.V().select("c").map(select("c").map(select("c"))).select("b"), "[[b, c], [[b, c], [[b]]], []]", null},
                {__.V().select("a").map(select("c").map(select("b"))).select("c"),
                        "[[b, c], [[b, c], [[c]]], []]", null},
                {__.V().select("a").map(select("b").repeat(select("c"))).select("a"),
                        "[[a, b, c], [[a, c], [[a, c]]], []]", null},
                {__.V().out("created").project("a", "b").by("name").by(__.in("created").count()).order().by(select("b")).select("a"), "[[[a]], []]", null},
                {__.order().by("weight", Order.desc).store("w").by("weight").filter(values("weight").as("cw").
                        select("w").by(limit(Scope.local, 1)).as("mw").where("cw", eq("mw"))).project("from", "to", "weight").by(__.outV()).by(__.inV()).by("weight"),
                        "[[[cw, mw], []]]", null},
                {__.V().limit(1).as("z").out().repeat(store("seen").out().where(without("seen"))).until(where(eq("z"))),
                        "[[[z, seen]], [[z, seen]]]", null},
                {__.V().as("a").optional(bothE().dedup().as("b")).
                        choose(select("b"), select("a","b"), project("a").by(select("a"))),
                        "[[[a, b]], [[a, b]], [[[a, b]]], [[a, b]]]", null},
                {__.V().as("a").out().where(neq("a")).program(labeledPathVertexProgram), PATH_RETRACTION_STRATEGY_DISABLED, null},
                {__.V().as("a").out().where(neq("a")).program(pathVertexProgram).select("a"), PATH_RETRACTION_STRATEGY_DISABLED, null},
                {__.V().as("a").out().program(emptyRequirementsVertexProgram).select("a"), "[[]]", null},
                {__.V().as("a").out().as("b").where(__.as("b").in().count().is(eq(3)).or().where(
                        __.as("b").out("created").and().as("b").has(T.label, "person"))).select("a", "b"),
                        "[[a, b], [[[a, b]]], []]", null}
        });
    }
}