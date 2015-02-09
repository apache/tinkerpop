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
package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphsTest {
    @Test
    @Ignore("Does not work on marko's local machine for some reason -- good for stephen and travis-ci")
    public void shouldReturnGraphs() {
        final Settings settings = Settings.read(GraphsTest.class.getResourceAsStream("gremlin-server-integration.yaml"));
        final Graphs graphs = new Graphs(settings);
        final Map<String, Graph> m = graphs.getGraphs();

        assertNotNull(m);
        assertTrue(m.containsKey("g"));
        assertTrue(m.get("g") instanceof TinkerGraph);
    }
}
