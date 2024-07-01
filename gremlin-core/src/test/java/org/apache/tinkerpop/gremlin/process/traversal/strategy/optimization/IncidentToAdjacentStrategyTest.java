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
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(Parameterized.class)
public class IncidentToAdjacentStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal.Admin original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        Function<Traverser<Vertex>, Vertex> lambda = Traverser::get; // to ensure same hashCode
        return Arrays.asList(new Traversal[][]{
                {__.outE().inV(), __.out()},
                {__.inE().outV(), __.in()},
                {__.bothE().otherV(), __.both()},
                {__.outE().outV(), __.outE().outV()},
                {__.inE().inV(), __.inE().inV()},
                {__.bothE().bothV(), __.bothE().bothV()},
                {__.bothE().inV(), __.bothE().inV()},
                {__.bothE().outV(), __.bothE().outV()},
                {__.outE().otherV(), __.out()},
                {__.inE().otherV(), __.in()},
                {__.outE().as("a").inV(), __.outE().as("a").inV()}, // todo: this can be optimized, but requires a lot more checks
                {__.outE().inV().path(), __.outE().inV().path()},
                {__.outE().inV().simplePath(), __.outE().inV().simplePath()},
                {__.outE().inV().tree(), __.outE().inV().tree()},
                {__.outE().inV().map(lambda), __.outE().inV().map(lambda)},
                {__.union(__.outE().inV(), __.inE().outV()).path(), __.union(__.outE().inV(), __.inE().outV()).path()},
                {__.as("a").outE().inV().as("b"), __.as("a").out().as("b")}});
    }

    @Test
    public void doTest() {
        final String repr = original.getGremlinLang().getGremlin("__");
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(IncidentToAdjacentStrategy.instance());
        this.original.asAdmin().setStrategies(strategies);
        this.original.asAdmin().applyStrategies();
        assertEquals(repr, this.optimized, this.original);
    }
}

