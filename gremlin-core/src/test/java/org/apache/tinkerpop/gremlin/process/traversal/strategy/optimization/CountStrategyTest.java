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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.GValueManagerVerifier;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gte;
import static org.apache.tinkerpop.gremlin.process.traversal.P.inside;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lte;
import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.outside;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.P.without;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.aggregate;
import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class CountStrategyTest {

    @RunWith(Parameterized.class)
    public static class StandardTest {
        @Parameterized.Parameter(value = 0)
        public Traversal.Admin original;

        @Parameterized.Parameter(value = 1)
        public Traversal optimized;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Traversal[][]{
                    {__.count().is(0), __.limit(1).count().is(0)},
                    {__.count().is(1), __.limit(2).count().is(1)},
                    {__.count().is(-1), __.limit(0).count().is(-1)}, // -1 for RangeGlobalStep means no upper limit
                    {__.count().is(-2), __.limit(0).count().is(-2)},
                    // the limit is kept at the root when the capped count can't pass the predicate: a count of 0 fails
                    // is(-2023) and between() excludes its upper bound of 10, so any count that does pass is exact
                    {__.count().is(-2023), __.limit(0).count().is(-2023)},
                    {__.count().is(P.between(0, 10)), __.limit(10).count().is(P.between(0, 10))},
                    {__.count().is(P.between(5, 10)), __.limit(10).count().is(P.between(5, 10))},
                    // limit(11) would cap e.g. 20 at 11, which passes gt(10) and would be returned in place of 20
                    {__.count().is(P.gt(10).or(lt(5))), __.count().is(P.gt(10).or(lt(5)))},
                    {__.where(__.count().is(P.gt(10).or(lt(5)))), __.where(__.limit(11).count().is(P.gt(10).or(lt(5))))},
                    {__.out().count().is(0), __.out().limit(1).count().is(0)},
                    {__.outE().count().is(lt(1)), __.outE().limit(1).count().is(lt(1))},
                    {__.both().count().is(lte(0)), __.both().limit(1).count().is(lte(0))},
                    {__.map(__.out().count().is(0)), __.map(__.out().limit(1).count().is(0))},
                    {__.map(__.outE().count().is(lt(1))), __.map(__.outE().limit(1).count().is(lt(1)))},
                    {__.map(__.both().count().is(lte(0))), __.map(__.both().limit(1).count().is(lte(0)))},
                    {__.filter(__.out().count().is(0)), __.not(__.out())},
                    {__.filter(__.outE().count().is(lt(1))), __.not(__.outE())},
                    {__.filter(__.both().count().is(lte(0))), __.not(__.both())},
                    {__.filter(__.out().out().count().is(0)), __.not(__.out().out())},
                    {__.local(aggregate("x")).count().is(0).as("a"), __.local(aggregate("x")).limit(1).count().is(0).as("a")},
                    {__.out().count().as("a").is(0), __.out().limit(1).count().as("a").is(0)},
                    {__.out().count().is(neq(4)), __.out().count().is(neq(4))}, // root: the count value is returned
                    {__.where(__.out().count().is(neq(4))), __.where(__.out().limit(5).count().is(neq(4)))},
                    {__.out().where(__.count().is(neq(4))), __.out().where(__.limit(5).count().is(neq(4)))},
                    {__.out().count().is(lte(3)), __.out().limit(4).count().is(lte(3))},
                    {__.out().count().is(lt(3)), __.out().limit(3).count().is(lt(3))},
                    {__.out().count().is(gt(2)), __.out().count().is(gt(2))}, // root: the count value is returned
                    {__.filter(__.out().count().is(gt(2))), __.filter(__.out().limit(3).count().is(gt(2)))},
                    {__.out().count().is(gte(2)), __.out().count().is(gte(2))}, // root: the count value is returned
                    {__.filter(__.out().count().is(gte(2))), __.filter(__.out().limit(2).count().is(gte(2)))},
                    {__.out().count().is(inside(2, 4)), __.out().limit(4).count().is(inside(2, 4))},
                    {__.out().count().is(outside(2, 4)), __.out().count().is(outside(2, 4))}, // root: the count value is returned
                    {__.where(__.out().count().is(outside(2, 4))), __.where(__.out().limit(5).count().is(outside(2, 4)))},
                    {__.out().count().is(within(2, 6, 4)), __.out().limit(7).count().is(within(2, 6, 4))},
                    {__.out().count().is(without(2, 6, 4)), __.out().count().is(without(2, 6, 4))}, // root: limit(7) passes and is returned
                    // without() only gives the same result for every count above its largest value, so the limit is max + 1
                    {__.where(__.out().count().is(without(2, 6, 4))), __.where(__.out().limit(7).count().is(without(2, 6, 4)))},
                    // not(lte(2)) is gt(2), which needs a limit of 3 like gt(2) does
                    {__.out().count().is(P.not(lte(2))), __.out().count().is(P.not(lte(2)))},
                    {__.where(__.out().count().is(P.not(lte(2)))), __.where(__.out().limit(3).count().is(P.not(lte(2))))},
                    // a chain of is() steps is limited by the largest limit of its predicates. if the chain rejects that
                    // cap, any count that passes is exact
                    {__.where(__.out().count().is(gt(1)).is(lt(3))), __.where(__.out().limit(3).count().is(gt(1)).is(lt(3)))},
                    {__.where(__.out().count().is(gte(2)).is(lte(2))), __.where(__.out().limit(3).count().is(gte(2)).is(lte(2)))},
                    {__.filter(__.out().count().is(neq(1)).is(lt(5))), __.filter(__.out().limit(5).count().is(neq(1)).is(lt(5)))},
                    {__.out().count().is(gt(1)).is(lt(3)), __.out().limit(3).count().is(gt(1)).is(lt(3))},
                    {__.map(__.out().count().is(gt(1)).is(lt(3))), __.map(__.out().limit(3).count().is(gt(1)).is(lt(3)))},
                    // if the chain accepts the cap, it can only be limited when the count isn't observed afterwards
                    {__.where(__.out().count().is(gt(1)).is(neq(2))), __.where(__.out().limit(3).count().is(gt(1)).is(neq(2)))},
                    {__.out().count().is(gt(1)).is(neq(2)), __.out().count().is(gt(1)).is(neq(2))},
                    {__.map(__.out().count().is(gt(1)).is(gt(2))), __.map(__.out().count().is(gt(1)).is(gt(2)))},
                    {__.where(__.out().count().is(gt(1)).is(gt(2)).as("a")), __.where(__.out().count().is(gt(1)).is(gt(2)).as("a"))},
                    // a traversal anywhere in the chain means the chain can't be reasoned about
                    {__.where(__.out().count().is(gt(1)).is(lt(__.constant(3L)))), __.where(__.out().count().is(gt(1)).is(lt(__.constant(3L))))},
                    // a capped count that passes is() must not be observed by later steps or the parent
                    // a label at the end of the where() child turns it into a WhereTraversalStep, which compares the
                    // count to the path, so the capped value would be observed
                    {__.where(__.out().count().is(gt(1)).as("a")), __.where(__.out().count().is(gt(1)).as("a"))},
                    // other labels in a filter-only child can't be read outside of it, so the limit is safe
                    {__.where(__.out().count().as("a").is(gt(1))), __.where(__.out().limit(2).count().as("a").is(gt(1)))},
                    {__.filter(__.out().count().as("a").is(gt(1))), __.filter(__.out().limit(2).count().as("a").is(gt(1)))},
                    {__.filter(__.out().count().is(gt(1)).as("a")), __.filter(__.out().limit(2).count().is(gt(1)).as("a"))},
                    {__.repeat(__.out()).until(__.out().count().is(gt(1)).is(lt(3))), __.repeat(__.out()).until(__.out().limit(3).count().is(gt(1)).is(lt(3)))},
                    {__.map(__.out().count().is(gt(1))), __.map(__.out().count().is(gt(1)))},
                    {__.flatMap(__.out().count().is(gte(2))), __.flatMap(__.out().count().is(gte(2)))},
                    {__.local(__.out().count().is(gt(1))), __.local(__.out().count().is(gt(1)))},
                    {__.project("a").by(__.out().count().is(gt(1))), __.project("a").by(__.out().count().is(gt(1)))},
                    {__.dedup().by(__.out().count().is(gt(1))), __.dedup().by(__.out().count().is(gt(1)))},
                    {__.aggregate("x").by(__.out().count().is(gt(1))), __.aggregate("x").by(__.out().count().is(gt(1)))},
                    {__.repeat(__.out().count().is(gt(1))).times(1), __.repeat(__.out().count().is(gt(1))).times(1)},
                    // traversal-valued predicates are resolved at runtime, so no safe limit can be derived, even in
                    // a filter-only child and even when the traversal reads a label on count()
                    {__.filter(__.out().count().is(gt(1).and(lt(__.constant(3L))))), __.filter(__.out().count().is(gt(1).and(lt(__.constant(3L)))))},
                    {__.filter(__.out().count().as("a").is(gt(1).and(lte(__.select("a"))))), __.filter(__.out().count().as("a").is(gt(1).and(lte(__.select("a")))))},
                    {__.out().count().is(gt(1).and(lte(__.constant(5L)))), __.out().count().is(gt(1).and(lte(__.constant(5L))))},
                    {__.filter(__.out().count().is(lt(__.constant(1L)))), __.filter(__.out().count().is(lt(__.constant(1L))))},
                    {__.filter(__.out().count().is(P.not(gt(__.constant(1L))))), __.filter(__.out().count().is(P.not(gt(__.constant(1L)))))},
                    // still optimized: only exact counts below the limit can pass the first is()
                    {__.where(__.out().count().is(lt(3)).is(gt(1))), __.where(__.out().limit(3).count().is(lt(3)).is(gt(1)))},
                    {__.out().count().is(lt(3)).is(gt(1)), __.out().limit(3).count().is(lt(3)).is(gt(1))},
                    {__.map(__.out().count().is(lte(3))), __.map(__.out().limit(4).count().is(lte(3)))},
                    // still optimized: the parent only checks whether anything passed
                    {__.where(__.out().count().is(gt(2))), __.where(__.out().limit(3).count().is(gt(2)))},
                    {__.not(__.out().count().is(gte(2))), __.not(__.out().limit(2).count().is(gte(2)))},
                    {__.sideEffect(__.out().count().is(gt(2))), __.sideEffect(__.out().limit(3).count().is(gt(2)))},
                    {__.repeat(__.out()).until(__.out().count().is(gt(2))), __.repeat(__.out()).until(__.out().limit(3).count().is(gt(2)))},
                    {__.repeat(__.out()).emit(__.out().count().is(gte(2))), __.repeat(__.out()).emit(__.out().limit(2).count().is(gte(2)))},
                    {__.out().until(__.out().count().is(gt(2))).repeat(__.out()), __.out().until(__.out().limit(3).count().is(gt(2))).repeat(__.out())},
                    {__.and(__.out().count().is(gt(1)), __.has("name")), __.and(__.out().limit(2).count().is(gt(1)), __.has("name"))},
                    {__.or(__.out().count().is(gte(3)), __.has("name")), __.or(__.out().limit(3).count().is(gte(3)), __.has("name"))},
                    {__.map(__.count().is(0)), __.map(__.limit(1).count().is(0))},
                    {__.flatMap(__.count().is(0)), __.flatMap(__.limit(1).count().is(0))},
                    {__.flatMap(__.count().is(0)).as("a"), __.flatMap(__.count().is(0)).as("a")}, // no change
                    {__.filter(__.count().is(0)).as("a"), __.not(__.identity()).as("a")},
                    {__.filter(__.count().is(0)), __.not(__.identity())},
                    {__.sideEffect(__.count().is(0)), __.sideEffect(__.not(__.identity()))},
                    {__.branch(__.count().is(0)), __.branch(__.limit(1).count().is(0))},
                    {__.count().is(0).local(aggregate("x")), __.limit(1).count().is(0).local(aggregate("x"))},
                    {__.repeat(__.out()).until(__.outE().count().is(0)), __.repeat(__.out()).until(__.not(__.outE()))},
                    {__.repeat(__.out()).until(__.out().out().values("name").count().is(0)), __.repeat(__.out()).until(__.out().out().not(__.values("name")))},
                    {__.repeat(__.out()).until(__.out().out().properties("age").has("x").count().is(0)), __.repeat(__.out()).until(__.out().out().not(__.properties("age").has("x")))},
                    {__.repeat(__.out()).emit(__.outE().count().is(0)), __.repeat(__.out()).emit(__.not(__.outE()))},
                    {__.where(__.outE().hasLabel("created").count().is(0)), __.not(__.outE().hasLabel("created"))},
                    {__.where(__.out().outE().hasLabel("created").count().is(0)), __.not(__.out().outE().hasLabel("created"))},
                    {__.where(__.out().outE().hasLabel("created").count().is(0).local(aggregate("x"))), __.where(__.out().outE().hasLabel("created").limit(1).count().is(0).local(aggregate("x")))},
                    {__.where(__.or(__.out("none").out().count().is(0), __.has("none"))), __.where(__.or(__.not(__.out("none").out()), __.has("none"))) },
                    {__.where(__.or(__.out("none").out().count().is(0), __.has("none").count().is(0))), __.where(__.or(__.not(__.out("none").out()), __.not(__.has("none")))) },
                    {__.where(__.or(__.out("none").out(), __.has("none").count().is(0))), __.where(__.or(__.out("none").out(), __.not(__.has("none")))) },
                    {__.filter(__.out().count().is(gt(-1))), __.filter(__.out().limit(0).count().is(gt(-1)))},
                    {__.filter(__.out().count().is(gte(-1))), __.filter(__.out().limit(0).count().is(gte(-1)))},
                    {__.filter(__.out().count().is(gte(0))), __.filter(__.out().limit(0).count().is(gte(0)))},
                    {__.filter(__.out().count().is(lt(-1))), __.filter(__.out().limit(0).count().is(lt(-1)))},
                    {__.filter(__.out().count().is(lt(0))), __.filter(__.out().limit(0).count().is(lt(0)))},
                    {__.filter(__.out().count().is(lt(2))), __.filter(__.out().limit(2).count().is(lt(2)))},
                    {__.filter(__.out().count().is(lte(-1))), __.filter(__.out().limit(0).count().is(lte(-1)))},
                    {__.filter(__.out().count().is(lte(1))), __.filter(__.out().limit(2).count().is(lte(1)))},
                    {__.filter(__.out().count().is(eq(-1))), __.filter(__.out().limit(0).count().is(eq(-1)))},
                    {__.filter(__.out().count().is(neq(-1))), __.filter(__.out().limit(0).count().is(neq(-1)))},
                    {__.filter(__.out().count().is(lte(1))), __.filter(__.out().limit(2).count().is(lte(1)))},
                    {__.filter(__.out().count().is(lt(-3).and(gt(0)))), __.filter(__.out().limit(1).count().is(lt(-3).and(gt(0))))},
                    {__.filter(__.out().count().is(lt(1).and(gt(-5)))), __.filter(__.out().limit(1).count().is(lt(1).and(gt(-5))))},
                    {__.filter(__.out().count().is(gt(-5).and(lt(1)))), __.filter(__.out().limit(1).count().is(gt(-5).and(lt(1))))},
                    {__.filter(__.out().count().is(gt(-5).and(gt(7)))), __.filter(__.out().limit(8).count().is(gt(-5).and(gt(7))))},
                    {__.filter(__.out().count().is(lt(1).or(gt(-5)))), __.filter(__.out().limit(1).count().is(lt(1).or(gt(-5))))},
                    {__.filter(__.out().count().is(lt(1).or(gt(3)))), __.filter(__.out().limit(4).count().is(lt(1).or(gt(3))))},
                    {__.filter(__.out().count().is(gt(3).or(lt(-51)))), __.filter(__.out().limit(4).count().is(gt(3).or(lt(-51))))},
                    {__.filter(__.out().count().is(gt(-5).or(lt(1)))), __.filter(__.out().limit(1).count().is(gt(-5).or(lt(1))))},
                    {__.filter(__.out().count().is(lt(10).and(gt(1)))), __.filter(__.out().limit(10).count().is(lt(10).and(gt(1))))},
                    {__.filter(__.out().count().is(gt(1).and(lt(10)))), __.filter(__.out().limit(10).count().is(gt(1).and(lt(10))))},
                    {__.filter(__.out().count().is(lt(10).or(gt(1)))), __.filter(__.out().limit(10).count().is(lt(10).or(gt(1))))},
                    {__.filter(__.out().count().is(gt(1).or(lt(10)))), __.filter(__.out().limit(10).count().is(gt(1).or(lt(10))))},
                    {__.filter(__.out().count().is(lt(5).and(gt(10)))), __.filter(__.out().limit(11).count().is(lt(5).and(gt(10))))},
                    {__.filter(__.out().count().is(gt(10).and(lt(5)))), __.filter(__.out().limit(11).count().is(gt(10).and(lt(5))))},
                    {__.filter(__.out().count().is(lt(-5).and(lt(1)))), __.filter(__.out().limit(1).count().is(lt(-5).and(lt(1))))},
                    {__.filter(__.out().count().is(lt(-5).and(lt(-1)))), __.filter(__.out().limit(0).count().is(lt(-5).and(lt(-1))))},
                    {__.filter(__.out().count().is(lt(-5).or(lt(-1)))), __.filter(__.out().limit(0).count().is(lt(-5).or(lt(-1))))},
                    {__.where(__.in("knows").count().is(inside(-1,1).and(lt(-1)))), __.where(__.in("knows").limit(1).count().is(inside(-1,1).and(lt(-1))))}, // TINKERPOP-2893
                    {__.where(__.inV().count().is(outside(-1,-2))), __.where(__.inV().limit(0).count().is(outside(-1,-2)))}, // TINKERPOP-2892
                    {__.filter(__.bothE().count().is(gt(0))), __.filter(__.bothE())},
                    {__.filter(__.bothE().count().is(gte(1))), __.filter(__.bothE())},
                    {__.filter(__.bothE().count().is(gt(1))), __.filter(__.bothE().limit(2).count().is(gt(1)))},
                    {__.filter(__.bothE().count().is(gte(2))), __.filter(__.bothE().limit(2).count().is(gte(2)))},
                    {__.and(__.out().count().is(0), __.in().count().is(0)), __.and(__.not(__.out()), __.not(__.in()))},
                    {__.and(__.out().count().is(0), __.in().count().is(1)), __.and(__.not(__.out()), __.in().limit(2).count().is(1))},
                    {__.and(__.out().count().is(1), __.in().count().is(0)), __.and(__.out().limit(2).count().is(1), __.not(__.in()))},
                    {__.and(__.out().out().count().is(0), __.in().count().is(0)), __.and(__.not(__.out().out()), __.not(__.in()))},
                    {__.or(__.out().count().is(0), __.in().count().is(0)), __.or(__.not(__.out()), __.not(__.in()))},
                    {__.path().filter(__.count().is(gte(0.5))).limit(5), __.path().identity().limit(5)}, // unfortunately we can't just remove the filter step
                    {__.path().filter(__.unfold().count().is(gte(0.5))), __.path().filter(__.unfold())},
                    {__.path().filter(__.unfold().count().is(gte(1.5))), __.path().filter(__.unfold().limit(2).count().is(gte(1.5)))},
            });
        }

        @Test
        public void doTest() {
            final String repr = original.getGremlinLang().getGremlin();
            GValueManagerVerifier.verify(original.asAdmin(), CountStrategy.instance())
                    .afterApplying()
                    .managerIsEmpty();
            assertEquals(repr, optimized, original);
        }
    }

    /**
     * Tests that {@link GValueManager} is properly maintaining state in cases where new {@link RangeGlobalStep}
     * instances are being introduced by CountStrategy.
     */
    @RunWith(Parameterized.class)
    public static class GValuePinnedBecauseItWasAccessedToInformTest {

        @Parameterized.Parameter(value = 0)
        public Traversal.Admin<?, ?> traversal;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    { __.count().is(GValue.of("x", 0)).asAdmin() },
                    { __.count().is(GValue.of("x", 1)).asAdmin() },
                    { __.count().is(P.between(GValue.of("x", 0), GValue.of("y", 10)))},
                    { __.out().count().is(gt(GValue.ofInteger("x", 1))).asAdmin() },
                    { __.out().count().is(lt(GValue.ofInteger("x", 1))).asAdmin() },
                    { __.out().count().is(gte(GValue.ofInteger("x", 1))).asAdmin() },
                    { __.out().count().is(lte(GValue.ofInteger("x", 1))).asAdmin() },
                    { __.out().count().is(eq(GValue.ofInteger("x", 1))).asAdmin() },
                    { __.out().count().is(neq(GValue.ofInteger("x", 1))).asAdmin() },
                    {__.count().is(GValue.of("x", 0))},
                    {__.count().is(GValue.of("x", 1))},
                    {__.count().is(GValue.of("x", -1))},
                    {__.count().is(GValue.of("x", -2))},
                    {__.count().is(GValue.of("x", -2023))},
                    {__.out().count().is(GValue.of("x", 0))},
                    {__.outE().count().is(lt(GValue.ofInteger("x", 1)))},
                    {__.both().count().is(lte(GValue.ofInteger("x", 0)))},
                    {__.map(__.out().count().is(GValue.of("x", 0)))},
                    {__.map(__.outE().count().is(lt(GValue.ofInteger("x", 1))))},
                    {__.map(__.both().count().is(lte(GValue.ofInteger("x", 0))))},
                    {__.filter(__.out().count().is(GValue.of("x", 0)))},
                    {__.filter(__.outE().count().is(lt(GValue.ofInteger("x", 1))))},
                    {__.filter(__.both().count().is(lte(GValue.ofInteger("x", 0))))},
                    {__.filter(__.out().out().count().is(GValue.of("x", 0)))},
                    {__.local(aggregate("x")).count().is(GValue.of("x", 0)).as("a")},
                    {__.out().count().as("a").is(GValue.of("x", 0))},
                    {__.out().count().is(neq(GValue.ofInteger("x", 4)))},
                    {__.out().count().is(lte(GValue.ofInteger("x", 3)))},
                    {__.out().count().is(lt(GValue.ofInteger("x", 3)))},
                    {__.out().count().is(gt(GValue.ofInteger("x", 2)))},
                    {__.out().count().is(gte(GValue.ofInteger("x", 2)))},
                    {__.out().count().is(inside(GValue.ofInteger("x", 2), GValue.ofInteger("y", 4)))},
                    {__.out().count().is(outside(GValue.ofInteger("x", 2), GValue.ofInteger("y", 4)))},
                    {__.out().count().is(within(GValue.ofInteger("x", 2), GValue.ofInteger("y", 6), GValue.ofInteger("z", 4)))},
                    {__.out().count().is(without(GValue.ofInteger("x", 2), GValue.ofInteger("y", 6), GValue.ofInteger("z", 4)))},
                    {__.map(__.count().is(GValue.of("x", 0)))},
                    {__.flatMap(__.count().is(GValue.of("x", 0)))},
                    {__.filter(__.count().is(GValue.of("x", 0))).as("a")},
                    {__.filter(__.count().is(GValue.of("x", 0)))},
                    {__.sideEffect(__.count().is(GValue.of("x", 0)))},
                    {__.branch(__.count().is(GValue.of("x", 0)))},
                    {__.count().is(GValue.of("x", 0)).local(aggregate("x"))},
                    {__.repeat(__.out()).until(__.outE().count().is(GValue.of("x", 0)))},
                    {__.repeat(__.out()).until(__.out().out().values("name").count().is(GValue.of("x", 0)))},
                    {__.repeat(__.out()).until(__.out().out().properties("age").has("x").count().is(GValue.of("x", 0)))},
                    {__.repeat(__.out()).emit(__.outE().count().is(GValue.of("x", 0)))},
                    {__.where(__.outE().hasLabel("created").count().is(GValue.of("x", 0)))},
                    {__.where(__.out().outE().hasLabel("created").count().is(GValue.of("x", 0)))},
                    {__.where(__.out().outE().hasLabel("created").count().is(GValue.of("x", 0)).local(aggregate("x")))},
                    {__.where(__.or(__.out("none").out().count().is(GValue.of("x", 0)), __.has("none")))},
                    {__.where(__.or(__.out("none").out().count().is(GValue.of("x", 0)), __.has("none").count().is(GValue.of("x", 0))))},
                    {__.where(__.or(__.out("none").out(), __.has("none").count().is(GValue.of("x", 0))))},
                    {__.filter(__.out().count().is(gt(GValue.ofInteger("x", -1))))},
                    {__.filter(__.out().count().is(gte(GValue.ofInteger("x", -1))))},
                    {__.filter(__.out().count().is(gte(GValue.ofInteger("x", 0))))},
                    {__.filter(__.out().count().is(lt(GValue.ofInteger("x", -1))))},
                    {__.filter(__.out().count().is(lt(GValue.ofInteger("x", 0))))},
                    {__.filter(__.out().count().is(lt(GValue.ofInteger("x", 2))))},
                    {__.filter(__.out().count().is(lte(GValue.ofInteger("x", -1))))},
                    {__.filter(__.out().count().is(lte(GValue.ofInteger("x", 1))))},
                    {__.filter(__.out().count().is(eq(GValue.ofInteger("x", -1))))},
                    {__.filter(__.out().count().is(neq(GValue.ofInteger("x", -1))))},
                    {__.filter(__.out().count().is(lte(GValue.ofInteger("x", 1))))},
                    {__.filter(__.out().count().is(lt(GValue.ofInteger("x", -3)).and(gt(GValue.ofInteger("y", 0)))))},
                    {__.filter(__.out().count().is(lt(GValue.ofInteger("x", 1)).and(gt(GValue.ofInteger("y", -5)))))},
                    {__.filter(__.out().count().is(gt(GValue.ofInteger("x", -5)).and(lt(GValue.ofInteger("y", 1)))))},
                    {__.filter(__.out().count().is(gt(GValue.ofInteger("x", -5)).and(gt(GValue.ofInteger("y", 7)))))},
                    {__.filter(__.out().count().is(lt(GValue.ofInteger("x", 1)).or(gt(GValue.ofInteger("y", -5)))))},
                    {__.filter(__.out().count().is(lt(GValue.ofInteger("x", 1)).or(gt(GValue.ofInteger("y", 3)))))},
                    {__.filter(__.out().count().is(gt(GValue.ofInteger("x", 3)).or(lt(GValue.ofInteger("y", -51)))))},
                    {__.filter(__.out().count().is(gt(GValue.ofInteger("x", -5)).or(lt(GValue.ofInteger("y", 1)))))},
                    {__.filter(__.out().count().is(lt(GValue.ofInteger("x", 10)).and(gt(GValue.ofInteger("y", 1)))))},
                    {__.filter(__.out().count().is(gt(GValue.ofInteger("x", 1)).and(lt(GValue.ofInteger("y", 10)))))},
                    {__.filter(__.out().count().is(lt(GValue.ofInteger("x", 10)).or(gt(GValue.ofInteger("y", 1)))))},
                    {__.filter(__.out().count().is(gt(GValue.ofInteger("x", 1)).or(lt(GValue.ofInteger("y", 10)))))},
                    {__.filter(__.out().count().is(lt(GValue.ofInteger("x", 5)).and(gt(GValue.ofInteger("y", 10)))))},
                    {__.filter(__.out().count().is(gt(GValue.ofInteger("x", 10)).and(lt(GValue.ofInteger("y", 5)))))},
                    {__.filter(__.out().count().is(lt(GValue.ofInteger("x", -5)).and(lt(GValue.ofInteger("y", 1)))))},
                    {__.filter(__.out().count().is(lt(GValue.ofInteger("x", -5)).and(lt(GValue.ofInteger("y", -1)))))},
                    {__.filter(__.out().count().is(lt(GValue.ofInteger("x", -5)).or(lt(GValue.ofInteger("y", -1)))))},
                    {__.where(__.in("knows").count().is(inside(GValue.ofInteger("x", -1), GValue.ofInteger("y", 1)).and(lt(GValue.ofInteger("x", -1)))))},
                    {__.where(__.inV().count().is(outside(GValue.ofInteger("x", -1), GValue.ofInteger("y", -2))))},
                    {__.filter(__.bothE().count().is(gt(GValue.ofInteger("x", 0))))},
                    {__.filter(__.bothE().count().is(gte(GValue.ofInteger("x", 1))))},
                    {__.filter(__.bothE().count().is(gt(GValue.ofInteger("x", 1))))},
                    {__.filter(__.bothE().count().is(gte(GValue.ofInteger("x", 2))))},
                    {__.and(__.out().count().is(GValue.of("x", 0)), __.in().count().is(GValue.of("x", 0)))},
                    {__.and(__.out().count().is(GValue.of("x", 0)), __.in().count().is(GValue.of("y", 1)))},
                    {__.and(__.out().count().is(GValue.of("x", 1)), __.in().count().is(GValue.of("y", 0)))},
                    {__.and(__.out().out().count().is(GValue.of("x", 0)), __.in().count().is(GValue.of("x", 0)))},
                    {__.or(__.out().count().is(GValue.of("x", 0)), __.in().count().is(GValue.of("x", 0)))},
                    {__.path().filter(__.count().is(gte(GValue.ofDouble("x", 0.5)))).limit(5)},
                    {__.path().filter(__.unfold().count().is(gte(GValue.ofDouble("x", 0.5))))},
                    {__.path().filter(__.unfold().count().is(gte(GValue.ofDouble("x", 1.5))))},
                    {__.project("a").by(__.count().is(GValue.of("x", 0)))},
                    {__.select("a").by(__.count().is(GValue.of("x", 0)))},
                    {__.group().by(__.count().is(GValue.of("x", 0)))},
                    {__.choose(__.count().is(GValue.of("x", 0)), __.out(), __.in())},
                    {__.count().as("c").is(GValue.of("x", 0))},
                    {__.union(__.count().is(GValue.of("x", 0)), __.out().count())},
                    {__.coalesce(__.count().is(GValue.of("x", 0)), __.out().count())},

                    // This case may be controversial, as the GValues are accessed by the strategy, but ultimately the traversal
                    // is not modified. The variables are pinned regardless as those variables could later be updated to values
                    // in which the strategy is active. Leaving the variables unpinned in such a case would not lead to an
                    // incorrect traversal, however it may result in certain cached traversals missing out on the CountStrategy
                    // optimizations.
                    {__.count().is(P.within(GValue.of("x", "string"), GValue.of("y", "another")))},
            });
        }

        @Test
        public void shouldMaintainGValueManagerState() {
            // After applying strategies, all IsSteps should have no GValue references
            GValueManagerVerifier.verify(traversal, CountStrategy.instance()).
                    beforeApplying().
                    stepsOfClassAreParameterized(true, IsStepContract.class).
                    afterApplying().
                    allGValuesArePinned();
        }
    }

    /**
     * Tests that {@link GValueManager} is properly maintaining state in cases strategy logic does not depend on
     * the content of the {@link GValue}.
     */
    @RunWith(Parameterized.class)
    public static class GValuePreservedSinceStrategyWasNotInformedByGValueTest {

        @Parameterized.Parameter(value = 0)
        public Traversal.Admin<?, ?> traversal;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {__.flatMap(__.count().is(GValue.of("x", 0))).as("a")},
                    {__.limit(5).count().is(GValue.of("x", 0))},
                    {__.range(1, 5).count().is(GValue.of("x", 0))},
            });
        }

        @Test
        public void shouldMaintainGValueManagerState() {
            // After applying strategies, all IsSteps should have no GValue references
            GValueManagerVerifier.verify(traversal, CountStrategy.instance()).
                    beforeApplying().
                    stepsOfClassAreParameterized(true, IsStepContract.class).
                    afterApplying().
                    variablesArePreserved();
        }
    }
}