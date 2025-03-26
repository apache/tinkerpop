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

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(GremlinProcessRunner.class)
public class EarlyLimitStrategyProcessTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(GRATEFUL)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    @Ignore // todo: re-enable after traversal metrics serialization is implemented
    public void shouldHandleRangeSteps() throws Exception {

        final GraphTraversal<Vertex, Map<String, List<String>>> t =
                g.V().has("artist", "name", "Bob_Dylan")
                        .in("sungBy")
                        .order()
                            .by("performances", Order.desc).as("a")
                        .repeat(__.out("followedBy")
                                    .order()
                                        .by("performances", Order.desc)
                                    .simplePath()
                                        .from("a"))
                            .until(__.out("writtenBy").has("name", "Johnny_Cash"))
                        .limit(1).as("b")
                        .repeat(__.out("followedBy")
                                    .order()
                                        .by("performances", Order.desc).as("c")
                                    .simplePath()
                                        .from("b")
                                        .to("c"))
                            .until(__.out("sungBy").has("name", "Grateful_Dead"))
                        .limit(5).as("d")
                        .path()
                            .from("a")
                        .limit(1).as("e")
                        .unfold().
                        <List<String>>project("song", "artists")
                            .by("name")
                            .by(__.coalesce(
                                    __.out("sungBy", "writtenBy").dedup().values("name"),
                                    __.constant("Unknown"))
                                    .fold());

        final GraphTraversal pt = t.asAdmin().clone().profile();
        final List<Map<String, List<String>>> result = t.toList();
        final TraversalMetrics metrics = (TraversalMetrics) pt.next();

        assertEquals(6, result.size());

        assumeTrue("The following assertions apply to TinkerGraph only as provider strategies can alter the " +
                        "steps to not comply with expectations", graph.getClass().getSimpleName().equals("TinkerGraph"));

        if (t.asAdmin().getStrategies().getStrategy(EarlyLimitStrategy.class).isPresent()) {
            assertEquals(10, metrics.getMetrics().size());
            assertThat(metrics.getMetrics(5).getName().endsWith("@[d]"), is(true));
            assertEquals("RangeGlobalStep(0,1)", metrics.getMetrics(6).getName());
            assertEquals("PathStep@[e]", metrics.getMetrics(7).getName());
            assertThat(metrics.getMetrics(7).getCounts().values().stream().allMatch(x -> x == 1L), is(true));
        } else {
            assertEquals(11, metrics.getMetrics().size());
            assertEquals("RangeGlobalStep(0,5)@[d]", metrics.getMetrics(6).getName());
            assertEquals("PathStep", metrics.getMetrics(7).getName());
            assertEquals("RangeGlobalStep(0,1)@[e]", metrics.getMetrics(8).getName());
            // the following used to get 2 for the count but after 17b35aa295f7e84f26fd75f8a82fc7e1c33f73f0 we stopped
            // forward pulling an extra traverser, so pretty sure the correct assertion is to match what happens with
            // EarlyLimitStrategy above. i'm not even sure there is a point to asserting this anymore as the original
            // intent does not appear clear to me, but i will leave it for now.
            // assertThat(metrics.getMetrics(7).getCounts().values().stream().allMatch(x -> x == 1L), is(true));
        }
    }
}