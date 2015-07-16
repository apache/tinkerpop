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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.mindrot.jbcrypt.BCrypt;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tinkerpop.gremlin.server.auth.groovy.plugin.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.server.auth.groovy.plugin.CredentialGraphTokens.PROPERTY_USERNAME;
import static org.apache.tinkerpop.gremlin.server.auth.groovy.plugin.CredentialGraphTokens.VERTEX_LABEL_USER;

/**
 * A DSL for managing a "credentials graph" used by Gremlin Server for simple authentication functions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CredentialGraph {

    private final Graph graph;
    private final GraphTraversalSource g;

    public CredentialGraph(final Graph graph) {
        this.graph = graph;
        g = graph.traversal();
    }

    /**
     * Finds a user by username and return {@code null} if one could not be found.
     *
     * @throws IllegalStateException if there is more than one user with a particular username.
     */
    public Vertex findUser(final String username) {
        final GraphTraversal<Vertex,Vertex> t = g.V().has(PROPERTY_USERNAME, username);
        final Vertex v = t.hasNext() ? t.next() : null;
        if (t.hasNext()) throw new IllegalStateException(String.format("Multiple users with username %s", username));
        return v;
    }

    /**
     * Creates a new user.
     *
     * @return the newly created user vertex
     */
    public Vertex createUser(final String username, final String password) {
        if (findUser(username) != null) throw new IllegalStateException("User with this name already exists");
        return graph.addVertex(T.label, VERTEX_LABEL_USER,
                               PROPERTY_USERNAME, username,
                               PROPERTY_PASSWORD, BCrypt.hashpw(password, BCrypt.gensalt()));
    }

    /**
     * Removes a user by name.
     *
     * @return the number of users removed (which should be one or zero)
     */
    public long removeUser(final String username) {
        final AtomicInteger counter = new AtomicInteger(0);
        g.V().has(PROPERTY_USERNAME, username).sideEffect(v -> counter.incrementAndGet()).drop().iterate();
        return counter.get();
    }

    public static CredentialGraph credentials(final Graph graph) {
        return new CredentialGraph(graph);
    }
}
