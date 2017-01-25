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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
    public void shouldNotAlterGraphConfiguration() {
        final String uuid = UUID.randomUUID().toString();
        final Configuration graphConfiguration = graph.configuration();
        assertEquals(ConfigurationConverter.getMap(graphConfiguration), ConfigurationConverter.getMap(graph.configuration()));
        assertFalse(graphConfiguration.containsKey("graphActorsTest.uuid-" + uuid));
        final GraphActors actors = graphProvider.getGraphActors(graph);
        actors.configure("graphActorsTest.uuid-" + uuid, "bloop");
        actors.workers(1);
        final Configuration actorsConfiguration = actors.configuration();
        assertEquals("bloop", actorsConfiguration.getString("graphActorsTest.uuid-" + uuid));
        assertEquals(1, actorsConfiguration.getInt(GraphActors.GRAPH_ACTORS_WORKERS));
        ///
        assertEquals(ConfigurationConverter.getMap(graphConfiguration), ConfigurationConverter.getMap(graph.configuration()));
        assertFalse(graphConfiguration.containsKey("graphActorsTest.uuid-" + uuid));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldHaveProperWorkerCount() {
        final GraphActors actors = graphProvider.getGraphActors(graph);
        for (int i = 1; i < 10; i++) {
            assertEquals(6L, g.withProcessor(actors.workers(i)).V().count().next().longValue());
            assertEquals(i, actors.configuration().getProperty(GraphActors.GRAPH_ACTORS_WORKERS));
        }
    }

    @Test
    @Ignore
    public void shouldSetupAndTerminateProperly() throws Exception {
        for (int i = 1; i < 10; i++) {
            final GraphActors<List<Integer>> actors = graphProvider.getGraphActors(graph);
            final List<Integer> counts = actors.workers(i).program(new TestSetupTerminateActorProgram()).submit(graph).get().getResult();
            assertEquals(i, counts.get(0).intValue());
            assertEquals(i, counts.get(1).intValue());
            assertEquals(1, counts.get(2).intValue());
            assertEquals(1, counts.get(3).intValue());
        }
    }

    @Test
    public void shouldSupportMessageCombiners() throws Exception {
        for (int i = 1; i < 10; i++) {
            final GraphActors actors = graphProvider.getGraphActors(graph);
            actors.workers(i).program(new TestMessageCombinersActorProgram()).submit(graph).get();
        }
    }
}