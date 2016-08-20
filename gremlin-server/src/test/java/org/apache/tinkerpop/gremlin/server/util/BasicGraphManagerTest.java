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
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import javax.script.Bindings;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BasicGraphManagerTest {

    @Test
    public void shouldReturnGraphs() {
        final Settings settings = Settings.read(BasicGraphManagerTest.class.getResourceAsStream("../gremlin-server-integration.yaml"));
        final GraphManager graphManager = new BasicGraphManager(settings);
        final Map<String, Graph> m = graphManager.getGraphs();

        assertNotNull(m);
        assertEquals(1, m.size());
        assertThat(m.containsKey("graph"), is(true));
        assertThat(m.get("graph"), instanceOf(TinkerGraph.class));
    }

    @Test
    public void shouldGetAsBindings() {
        final Settings settings = Settings.read(BasicGraphManagerTest.class.getResourceAsStream("../gremlin-server-integration.yaml"));
        final GraphManager graphManager = new BasicGraphManager(settings);
        final Bindings bindings = graphManager.getAsBindings();

        assertNotNull(bindings);
        assertEquals(1, bindings.size());
        assertThat(bindings.get("graph"), instanceOf(TinkerGraph.class));
        assertThat(bindings.containsKey("graph"), is(true));
    }
}
