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
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.Operator.assign;
import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(Parameterized.class)
public class ByModulatorOptimizationStrategyTest {

    @Parameterized.Parameter
    public Traversal original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        final List<Object[]> result = new ArrayList<>();
        final GraphTraversal[] baseTraversals = new GraphTraversal[]{
                __.aggregate("x"),
                __.dedup(),
                __.dedup("a"),
                __.group(),
                __.group("x"),
                __.groupCount(),
                __.groupCount("x"),
                __.order(),
                __.order(Scope.local),
                __.project("a"),
                __.path(),
                __.path().from("a").to("b"),
                __.sack(assign),
                __.sample(10),
                __.select("a"),
                __.select("a", "b"),
                __.store("x"),
                __.tree(),
                __.tree("x"),
                __.where(P.eq("a")),
                __.where("a", P.eq("b"))
        };

        for (final Traversal traversal : baseTraversals) {
            result.add(new Traversal[]{
                    ((GraphTraversal<?, ?>) traversal.asAdmin().clone()).by(__.values("name")),
                    ((GraphTraversal) traversal.asAdmin().clone()).by("name"),
            });
            result.add(new Traversal[]{
                    ((GraphTraversal<?, ?>) traversal.asAdmin().clone()).by(__.id()),
                    ((GraphTraversal) traversal.asAdmin().clone()).by(T.id),
            });
            result.add(new Traversal[]{
                    ((GraphTraversal<?, ?>) traversal.asAdmin().clone()).by(__.label()),
                    ((GraphTraversal) traversal.asAdmin().clone()).by(T.label),
            });
            /*result.add(new Traversal[]{
                    ((GraphTraversal<?, ?>) traversal.asAdmin().clone()).by(__.key()),
                    ((GraphTraversal) traversal.asAdmin().clone()).by(T.key),
            });
            result.add(new Traversal[]{
                    ((GraphTraversal<?, ?>) traversal.asAdmin().clone()).by(__.value()),
                    ((GraphTraversal) traversal.asAdmin().clone()).by(T.value),
            });*/
            result.add(new Traversal[]{
                    ((GraphTraversal<?, ?>) traversal.asAdmin().clone()).by(__.identity()),
                    ((GraphTraversal) traversal.asAdmin().clone()).by(),
            });
        }

        result.add(new Traversal[]{
                __.project("a", "b", "c", "d", "e")
                        .by(__.values("name"))
                        .by(__.id())
                        .by(__.label())
                        .by(__.identity())
                        .by(__.outE().count()),
                __.project("a", "b", "c", "d", "e")
                        .by("name")
                        .by(T.id)
                        .by(T.label)
                        .by()
                        .by(__.outE().count())
        });

        return result;
    }

    private void applyByModulatorOptimizationStrategy(final Traversal traversal) {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(ByModulatorOptimizationStrategy.instance());
        traversal.asAdmin().setStrategies(strategies);
        traversal.asAdmin().applyStrategies();
    }

    @Test
    public void doTest() {
        applyByModulatorOptimizationStrategy(original);
        assertEquals(optimized, original);
    }
}