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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gte;
import static org.apache.tinkerpop.gremlin.process.traversal.P.inside;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lte;
import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.outside;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.P.without;
import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class RangeByIsCountStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        return Arrays.asList(new Traversal[][]{
                {__.count().is(0), __.limit(1).count().is(0)},
                {__.count().is(1), __.limit(2).count().is(1)},
                {__.out().count().is(0), __.out().limit(1).count().is(0)},
                {__.outE().count().is(lt(1)), __.outE().limit(1).count().is(lt(1))},
                {__.both().count().is(lte(0)), __.both().limit(1).count().is(lte(0))},
                {__.map(__.out().count().is(0)), __.map(__.out().limit(1).count().is(0))},
                {__.map(__.outE().count().is(lt(1))), __.map(__.outE().limit(1).count().is(lt(1)))},
                {__.map(__.both().count().is(lte(0))), __.map(__.both().limit(1).count().is(lte(0)))},
                {__.filter(__.out().count().is(0)), __.not(__.out())},
                {__.filter(__.outE().count().is(lt(1))), __.not(__.outE())},
                {__.filter(__.both().count().is(lte(0))), __.not(__.both())},
                {__.store("x").count().is(0).as("a"), __.store("x").limit(1).count().is(0).as("a")},
                {__.out().count().as("a").is(0), __.out().limit(1).count().as("a").is(0)},
                {__.out().count().is(neq(4)), __.out().limit(5).count().is(neq(4))},
                {__.out().count().is(lte(3)), __.out().limit(4).count().is(lte(3))},
                {__.out().count().is(lt(3)), __.out().limit(3).count().is(lt(3))},
                {__.out().count().is(gt(2)), __.out().limit(3).count().is(gt(2))},
                {__.out().count().is(gte(2)), __.out().limit(2).count().is(gte(2))},
                {__.out().count().is(inside(2, 4)), __.out().limit(4).count().is(inside(2, 4))},
                {__.out().count().is(outside(2, 4)), __.out().limit(5).count().is(outside(2, 4))},
                {__.out().count().is(within(2, 6, 4)), __.out().limit(7).count().is(within(2, 6, 4))},
                {__.out().count().is(without(2, 6, 4)), __.out().limit(6).count().is(without(2, 6, 4))},
                {__.map(__.count().is(0)), __.map(__.limit(1).count().is(0))},
                {__.flatMap(__.count().is(0)), __.flatMap(__.limit(1).count().is(0))},
                {__.flatMap(__.count().is(0)).as("a"), __.flatMap(__.count().is(0)).as("a")},
                {__.filter(__.count().is(0)).as("a"), __.not(__.identity()).as("a")},
                {__.filter(__.count().is(0)), __.not(__.identity())},
                {__.sideEffect(__.count().is(0)), __.sideEffect(__.not(__.identity()))},
                {__.branch(__.count().is(0)), __.branch(__.limit(1).count().is(0))},
                {__.count().is(0).store("x"), __.limit(1).count().is(0).store("x")},
                {__.repeat(__.out()).until(__.outE().count().is(0)), __.repeat(__.out()).until(__.not(__.outE()))},
                {__.repeat(__.out()).emit(__.outE().count().is(0)), __.repeat(__.out()).emit(__.not(__.outE()))},
                {__.where(__.outE().hasLabel("created").count().is(0)), __.not(__.outE().hasLabel("created"))},
                {__.where(__.out().outE().hasLabel("created").count().is(0)), __.not(__.out().outE().hasLabel("created"))},
                {__.where(__.out().outE().hasLabel("created").count().is(0).store("x")), __.where(__.out().outE().hasLabel("created").limit(1).count().is(0).store("x"))},
                {__.filter(__.bothE().count().is(gt(0))), __.filter(__.bothE())},
                {__.filter(__.bothE().count().is(gte(1))), __.filter(__.bothE())},
                {__.filter(__.bothE().count().is(gt(1))), __.filter(__.bothE().limit(2).count().is(gt(1)))},
                {__.filter(__.bothE().count().is(gte(2))), __.filter(__.bothE().limit(2).count().is(gte(2)))},
                {__.and(__.out().count().is(0), __.in().count().is(0)), __.and(__.not(__.out()), __.not(__.in()))},
                {__.and(__.out().count().is(0), __.in().count().is(1)), __.and(__.not(__.out()), __.in().limit(2).count().is(1))},
                {__.and(__.out().count().is(1), __.in().count().is(0)), __.and(__.out().limit(2).count().is(1), __.not(__.in()))},
                {__.or(__.out().count().is(0), __.in().count().is(0)), __.or(__.not(__.out()), __.not(__.in()))},
                {__.path().filter(__.count().is(gte(0.5))).limit(5), __.path().identity().limit(5)}, // unfortunately we can't just remove the filter step
                {__.path().filter(__.unfold().count().is(gte(0.5))), __.path().filter(__.unfold())},
                {__.path().filter(__.unfold().count().is(gte(1.5))), __.path().filter(__.unfold().limit(2).count().is(gte(1.5)))},
        });
    }

    void applyRangeByIsCountStrategy(final Traversal traversal) {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(RangeByIsCountStrategy.instance());
        traversal.asAdmin().setStrategies(strategies);
        traversal.asAdmin().applyStrategies();
    }

    @Test
    public void doTest() {
        applyRangeByIsCountStrategy(original);
        assertEquals(optimized, original);
    }
}
