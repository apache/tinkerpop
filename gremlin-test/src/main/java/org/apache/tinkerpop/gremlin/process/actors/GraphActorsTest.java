/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.actors;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphActorsTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldHaveStandardStringRepresentation() {
        final GraphActors actors = graphProvider.getGraphActors(graph);
        assertEquals(StringFactory.graphActorsString(actors), actors.toString());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldHaveProperWorkerCounte() {
        final GraphActors actors = graphProvider.getGraphActors(graph);
        for (int i = 1; i < 10; i++) {
            assertEquals(6L, g.withProcessor(actors.workers(i)).V().count().next().longValue());
            assertEquals(i, actors.configuration().getProperty(GraphActors.GRAPH_ACTORS_WORKERS));

        }
    }
}