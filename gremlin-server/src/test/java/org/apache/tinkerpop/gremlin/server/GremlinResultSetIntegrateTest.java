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
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinResultSetIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private Cluster cluster;
    private Client client;

    @Override
    public Settings overrideSettings(final Settings settings) {
        settings.scriptEngines.get("gremlin-groovy").scripts = Arrays.asList("scripts/generate-modern.groovy");
        return settings;
    }

    @Before
    public void beforeTest() {
        cluster = Cluster.open();
        client = cluster.connect();
    }

    @After
    public void afterTest() {
        cluster.close();
    }

    @Test
    public void shouldHandleNullResult() throws Exception {
        final ResultSet results = client.submit("g.V().drop().iterate();null");
        assertNull(results.all().get().get(0).getObject());
    }

    @Test
    public void shouldHandleVoidResult() throws Exception {
        final ResultSet results = client.submit("g.V().drop().iterate()");
        assertEquals(0, results.all().get().size());
    }

    @Test
    public void shouldHandleEmptyResult() throws Exception {
        final ResultSet results = client.submit("g.V(100,1000,1000)");
        assertEquals(0, results.all().get().size());
    }

    @Test
    public void shouldHandleVertexResult() throws Exception {
        final ResultSet results = client.submit("g.V().next()");
        final Vertex v = results.all().get().get(0).getVertex();
        assertThat(v, instanceOf(DetachedVertex.class));
    }

    @Test
    public void shouldHandleVertexPropertyResult() throws Exception {
        final ResultSet results = client.submit("g.V().properties('name').next()");
        final VertexProperty<String> v = results.all().get().get(0).getVertexProperty();
        assertThat(v, instanceOf(DetachedVertexProperty.class));
    }

    @Test
    public void shouldHandleEdgeResult() throws Exception {
        final ResultSet results = client.submit("g.E().next()");
        final Edge e = results.all().get().get(0).getEdge();
        assertThat(e, instanceOf(DetachedEdge.class));
    }

    @Test
    public void shouldHandlePropertyResult() throws Exception {
        final ResultSet results = client.submit("g.E().properties('weight').next()");
        final Property<Double> p = results.all().get().get(0).getProperty();
        assertThat(p, instanceOf(DetachedProperty.class));
    }

    @Test
    public void shouldHandlePathResult() throws Exception {
        final ResultSet results = client.submit("g.V().out().path().next()");
        final Path p = results.all().get().get(0).getPath();
        assertThat(p, instanceOf(DetachedPath.class));
    }
}
