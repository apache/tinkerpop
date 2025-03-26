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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class MatchPredicateStrategyTest {

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
        strategies.addStrategies(MatchPredicateStrategy.instance());
        for (final TraversalStrategy strategy : this.otherStrategies) {
            strategies.addStrategies(strategy);
        }
        this.original.asAdmin().setStrategies(strategies);
        this.original.asAdmin().applyStrategies();
        assertEquals(repr, this.optimized, this.original);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        return Arrays.asList(new Object[][]{
                {__.out().match(as("a").has("name", "marko"), as("a").out().as("b")), __.out().has("name", "marko").as("a").match(as("a").out().as("b")), Collections.singletonList(InlineFilterStrategy.instance())}, // has() pull out
                {__.out().as("a").match(as("a").has("name", "marko"), as("a").out().as("b")), __.out().as("a").has("name", "marko").as("a").match(as("a").out().as("b")), Collections.singletonList(InlineFilterStrategy.instance())}, // has() pull out
                {__.out().as("a").out().match(as("a").has("name", "marko"), as("a").out().as("b")), __.out().as("a").out().match(as("a").has("name", "marko"), as("a").out().as("b")), Collections.emptyList()}, // no has() pull out
                {__.map(__.match(as("a").has("name", "marko"), as("a").out().as("b"))), __.map(__.match(as("a").has("name", "marko"), as("a").out().as("b"))), Collections.emptyList()}, // no has() pull out
                {__.out().as("c").match(as("a").has("name", "marko"), as("a").out().as("b")), __.out().as("c").match(as("a").has("name", "marko"), as("a").out().as("b")), Collections.emptyList()}, // no has() pull out
                /////////
                {__.match(as("a").out().as("b"), __.where(as("b").out("knows").as("c"))), __.match(as("a").out().as("b"), as("b").where(__.out("knows").as("c"))), Collections.emptyList()}, // make as().where()
                {__.match(as("a").out().as("b"), __.where("a", P.gt("b"))), __.match(as("a").out().as("b"), __.as("a").where(P.gt("b"))), Collections.emptyList()},  // make as().where()
                /////////
                {__.match(as("a").out().as("b")).where(as("b").filter(has("name")).out("knows").as("c")), __.match(as("a").out().as("b"), as("b").where(has("name").out("knows").as("c"))), Collections.singletonList(InlineFilterStrategy.instance())}, // make as().where()
                {__.match(as("a").out().as("b")).where("a", P.gt("b")), __.match(as("a").out().as("b"), as("a").where(P.gt("b"))), Collections.emptyList()},  // make as().where()
        });
    }

}
