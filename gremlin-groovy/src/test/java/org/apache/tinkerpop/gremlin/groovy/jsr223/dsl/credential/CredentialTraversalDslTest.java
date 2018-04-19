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
package org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CredentialTraversalDslTest {

    @Test
    public void shouldCreateUser() {
        final Graph graph = TinkerGraph.open();
        final CredentialTraversalSource g = graph.traversal(CredentialTraversalSource.class);
        final Vertex v = g.user("stephen", "secret").next();
        assertEquals("stephen", v.value("username"));
        assertEquals("user", v.label());
        assertNotEquals("secret", v.value("password"));  // hashed to something
        assertThat(v.value("password").toString().length(), greaterThan(0));
    }

    @Test
    public void shouldRemoveUser() {
        final Graph graph = TinkerGraph.open();
        final CredentialTraversalSource g = graph.traversal(CredentialTraversalSource.class);
        assertThat(graph.vertices().hasNext(), is(false));
        g.user("stephen", "secret").iterate();
        assertThat(graph.vertices().hasNext(), is(true));

        g.users("stephen").drop().iterate();
        assertThat(graph.vertices().hasNext(), is(false));
    }

    @Test
    public void shouldNotRemoveUser() {
        final Graph graph = TinkerGraph.open();
        final CredentialTraversalSource g = graph.traversal(CredentialTraversalSource.class);
        assertThat(graph.vertices().hasNext(), is(false));
        g.user("stephen", "secret").iterate();
        assertThat(graph.vertices().hasNext(), is(true));

        g.users("stephanie").drop().iterate();
        assertThat(graph.vertices().hasNext(), is(true));
    }

    @Test
    public void shouldFindUser() {
        final Graph graph = TinkerGraph.open();
        final CredentialTraversalSource g = graph.traversal(CredentialTraversalSource.class);
        assertThat(graph.vertices().hasNext(), is(false));
        g.user("marko", "secret").iterate();
        final Vertex stephen = g.user("stephen", "secret").next();
        g.user("daniel", "secret").iterate();
        assertThat(graph.vertices().hasNext(), is(true));

        assertEquals(stephen, g.users("stephen").next());
    }

    @Test
    public void shouldFindSeveralUsers() {
        final Graph graph = TinkerGraph.open();
        final CredentialTraversalSource g = graph.traversal(CredentialTraversalSource.class);
        assertThat(graph.vertices().hasNext(), is(false));
        g.user("marko", "secret").iterate();
        g.user("daniel", "secret").iterate();
        g.user("stephen", "secret").iterate();
        assertThat(graph.vertices().hasNext(), is(true));

        assertThat(g.users("stephen", "marko").values(PROPERTY_USERNAME).toList(), containsInAnyOrder("stephen", "marko"));
    }

    @Test
    public void shouldNotFindUser() {
        final Graph graph = TinkerGraph.open();
        final CredentialTraversalSource g = graph.traversal(CredentialTraversalSource.class);
        assertThat(graph.vertices().hasNext(), is(false));
        g.user("marko", "secret").iterate();
        g.user("daniel", "secret").iterate();
        g.user("stephen", "secret").iterate();
        assertThat(graph.vertices().hasNext(), is(true));

        assertThat(g.users("stephanie").hasNext(), is(false));
    }

    @Test
    public void shouldCountUsers() {
        final Graph graph = TinkerGraph.open();
        final CredentialTraversalSource g = graph.traversal(CredentialTraversalSource.class);
        assertThat(graph.vertices().hasNext(), is(false));
        g.user("marko", "secret").iterate();
        g.user("daniel", "secret").iterate();
        g.user("stephen", "secret").iterate();
        assertThat(graph.vertices().hasNext(), is(true));

        assertEquals(3, g.users().count().next().intValue());
    }

    @Test
    public void shouldUpdateUser() {
        final Graph graph = TinkerGraph.open();
        final CredentialTraversalSource g = graph.traversal(CredentialTraversalSource.class);
        final Vertex v = g.user("stephen", "secret").next();
        assertEquals("stephen", v.value("username"));
        assertEquals("user", v.label());
        assertNotEquals("secret", v.value("password"));  // hashed to something
        assertThat(v.value("password").toString().length(), greaterThan(0));

        final String hashOfSecret = v.value("password").toString();

        g.user("stephen", "new-secret").iterate();

        assertNotEquals(hashOfSecret, g.users("stephen").values("password").next());
        assertEquals(1, g.users("stephen").count().next().intValue());
    }
}
