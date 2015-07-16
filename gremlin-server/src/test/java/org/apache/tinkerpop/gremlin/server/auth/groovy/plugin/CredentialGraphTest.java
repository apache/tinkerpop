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
package org.apache.tinkerpop.gremlin.server.auth.groovy.plugin;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.server.auth.groovy.plugin.CredentialGraph.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CredentialGraphTest {
    private TinkerGraph graph;

    @Before
    public void setup() {
        graph = TinkerGraph.open();
        graph.createIndex("username", Vertex.class);
    }

    @Test
    public void shouldCreateUser() {
        final Vertex v = credentials(graph).createUser("stephen", "secret");
        assertEquals("stephen", v.value("username"));
        assertEquals("user", v.label());
        assertNotEquals("secret", v.value("password"));  // hashed to something
        assertThat(v.value("password").toString().length(), greaterThan(0));
    }

    @Test
    public void shouldRemoveUser() {
        assertThat(graph.vertices().hasNext(), is(false));
        credentials(graph).createUser("stephen", "secret");
        assertThat(graph.vertices().hasNext(), is(true));

        assertEquals(1, credentials(graph).removeUser("stephen"));
        assertThat(graph.vertices().hasNext(), is(false));
    }

    @Test
    public void shouldNotRemoveUser() {
        assertThat(graph.vertices().hasNext(), is(false));
        credentials(graph).createUser("stephen", "secret");
        assertThat(graph.vertices().hasNext(), is(true));

        assertEquals(0, credentials(graph).removeUser("stephanie"));
        assertThat(graph.vertices().hasNext(), is(true));
    }

    @Test
    public void shouldFindUser() {
        assertThat(graph.vertices().hasNext(), is(false));
        credentials(graph).createUser("marko", "secret");
        final Vertex stephen = credentials(graph).createUser("stephen", "secret");
        credentials(graph).createUser("daniel", "secret");
        assertThat(graph.vertices().hasNext(), is(true));

        assertEquals(stephen, credentials(graph).findUser("stephen"));
    }

    @Test
    public void shouldNotFindUser() {
        assertThat(graph.vertices().hasNext(), is(false));
        credentials(graph).createUser("marko", "secret");
        credentials(graph).createUser("stephen", "secret");
        credentials(graph).createUser("daniel", "secret");
        assertThat(graph.vertices().hasNext(), is(true));

        assertNull(credentials(graph).findUser("stephanie"));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIfFindingMultipleUsers() {
        assertThat(graph.vertices().hasNext(), is(false));
        credentials(graph).createUser("stephen", "secret");
        credentials(graph).createUser("stephen", "secret");
        assertThat(graph.vertices().hasNext(), is(true));

        assertNull(credentials(graph).findUser("stephen"));
    }
}
