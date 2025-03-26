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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class ConnectiveStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal.Admin original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    void applyConnectiveStrategy(final Traversal traversal) {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(ConnectiveStrategy.instance());
        traversal.asAdmin().setStrategies(strategies);
        traversal.asAdmin().applyStrategies();
    }

    @Test
    public void doTest() {
        final String repr = original.getGremlinLang().getGremlin("__");
        applyConnectiveStrategy(original);
        assertEquals(repr, optimized, original);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        return Arrays.asList(new Traversal[][]{
                {__.has("name", "marko").and().has("name", "marko").and().has("name", "marko"), __.and(has("name", "marko"), __.has("name", "marko"), __.has("name", "marko"))},
                {__.has("name", "stephen").or().where(__.out("knows").has("name", "stephen")), __.or(__.has("name", "stephen"), __.where(__.out("knows").has("name", "stephen")))},
                {__.out("a").out("b").and().out("c").or().out("d"), __.or(__.and(__.out("a").out("b"), __.out("c")), __.out("d"))},
                {__.as("1").out("a").out("b").as("2").and().as("3").out("c").as("4").or().as("5").out("d").as("6"), __.or(__.and(__.as("1").out("a").out("b").as("2"), __.as("3").out("c").as("4")), __.as("5").out("d").as("6"))},
                {__.as("1").out("a").out("b").and().as("3").out("c").or().as("5").out("d"), __.or(__.and(__.as("1").out("a").out("b"), __.as("3").out("c")), __.as("5").out("d"))},
                {__.as("1").out("a").out("b").or().as("3").out("c").and().as("5").out("d"), __.or(__.as("1").out("a").out("b"), __.and(__.as("3").out("c"), __.as("5").out("d")))},
                {__.as("a").out().as("b").and().as("c").in().as("d"), __.and(__.as("a").out().as("b"), __.as("c").in().as("d"))},
                {__.union(__.as("a").out("l1").as("b").or().as("c").in("l2").as("d"), __.as("e").out("l3").as("f").and().as("g").in("l4").as("h")), __.union(__.or(__.as("a").out("l1").as("b"), __.as("c").in("l2").as("d")), __.and(__.as("e").out("l3").as("f"), __.as("g").in("l4").as("h")))},
                {__
                        .as("a1").out("a").as("a2").or()
                        .as("b1").out("b").as("b2").and()
                        .as("c1").out("c").as("c2").or()
                        .as("d1").out("d").as("d2").or()
                        .as("e1").out("e").as("e2").and()
                        .as("f1").out("f").as("f2").and()
                        .as("g1").out("g").as("g2").or()
                        .as("h1").out("h").as("h2").or(__
                                .as("i1").out("i").as("i2").or()
                                .as("j1").out("j").as("j2").and()
                                .as("k1").out("k").as("k2")).and()
                        .as("l1").out("l").as("l2").and()
                        .as("m1").out("m").as("m2").and()
                        .as("n1").out("n").as("n2"),
                        // EXPECT:
                        __.or(
                                __.as("a1").out("a").as("a2"),
                                __.and(
                                        __.as("b1").out("b").as("b2"),
                                        __.as("c1").out("c").as("c2")),
                                __.as("d1").out("d").as("d2"),
                                __.and(
                                        __.as("e1").out("e").as("e2"),
                                        __.as("f1").out("f").as("f2"),
                                        __.as("g1").out("g").as("g2")),
                                __.and(
                                        __.as("h1").out("h").as("h2").or(
                                                __.or(
                                                        __.as("i1").out("i").as("i2"),
                                                        __.and(
                                                                __.as("j1").out("j").as("j2"),
                                                                __.as("k1").out("k").as("k2")))),
                                __.as("l1").out("l").as("l2"),
                                __.as("m1").out("m").as("m2"),
                                __.as("n1").out("n").as("n2")))
                }
        });
    }
}
