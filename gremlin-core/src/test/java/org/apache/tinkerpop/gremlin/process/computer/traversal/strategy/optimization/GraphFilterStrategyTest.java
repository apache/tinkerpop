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

package org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class GraphFilterStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal original;

    @Parameterized.Parameter(value = 1)
    public Traversal edgeFilter;


    @Test
    public void doTest() {
        assertEquals(GraphFilterStrategy.instance().getEdgeFilter(this.original.asAdmin()), this.edgeFilter);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        return Arrays.asList(new Traversal[][]{
                {__.V().count(), __.bothE().limit(0)},
                {__.V().both().has("name"), null},
                {__.bothE(), null},
                {__.V().outE(), __.outE()},
                {__.V().in(), __.inE()},
                {__.V().local(__.outE("knows", "created").limit(10)), __.outE("knows", "created")},
                {__.out("created"), __.outE("created")},
                {__.in("created", "knows"), __.inE("created", "knows")},
                {__.V().both("created"), __.bothE("created")},
                {__.V().out("created").repeat(__.both("knows")).until(__.inE("bought", "likes")).outE("likes"), __.union(__.outE("created"), __.bothE("bought", "knows", "likes"))},
                {__.union(__.inE("created"), __.bothE()), null},
                {__.union(__.inE("created"), __.outE("created")), __.bothE("created")},
                {__.union(__.inE("knows"), __.outE("created")), __.union(__.outE("created"), __.bothE("knows"))},
                {__.union(__.inE("knows", "created"), __.outE("created")), __.bothE("knows", "created")},
                {__.V().out().out().match(
                        as("a").in("created").as("b"),
                        as("b").in("knows").as("c")).select("c").out("created").values("name"), null}
        });
    }
}
