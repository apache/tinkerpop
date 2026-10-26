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
package org.apache.tinkerpop.gremlin.fluency;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class CanonicalTest {
    private final Graph modern = TinkerFactory.createModern();
    private final GraphTraversalSource g = traversal().with(modern);
    private final Canonical canonical = new Canonical(modern);

    @Test
    public void shouldWriteTheSameSubgraphTheSameWay() {
        final Graph knows = (Graph) g.E().hasLabel("knows").subgraph("sg").cap("sg").next();
        final Graph knowsFromMarko = (Graph) g.V().has("name", "marko").outE("knows").subgraph("sg").cap("sg").next();
        assertThat(canonical.of(knows), is(canonical.of(knowsFromMarko)));
    }

    @Test
    public void shouldTellApartSubgraphsOfTheSameSize() {
        final Graph knows = (Graph) g.E().hasLabel("knows").subgraph("sg").cap("sg").next();
        final Graph joshCreated = (Graph) g.V().has("name", "josh").outE("created").subgraph("sg").cap("sg").next();

        // both have three vertices and two edges, which is all their toString shows
        assertThat(knows.toString(), is(joshCreated.toString()));
        assertThat(canonical.of(knows), is(not(canonical.of(joshCreated))));
    }
}
