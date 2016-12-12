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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.mindrot.BCrypt;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.drop;

/**
 * A DSL for managing a "credentials graph" used by Gremlin Server for simple authentication functions.  If the
 * {@link Graph} is transactional, new transactions will be started for each method call.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CredentialGraph {

    private final int BCRYPT_ROUNDS = 4;
    private final Graph graph;
    private final GraphTraversalSource g;
    private final boolean supportsTransactions;

    public CredentialGraph(final Graph graph) {
        this.graph = graph;
        g = graph.traversal();
        supportsTransactions = graph.features().graph().supportsTransactions();
    }

    /**
     * Finds a user by username and return {@code null} if one could not be found.
     *
     * @throws IllegalStateException if there is more than one user with a particular username.
     */
    public Vertex findUser(final String username) {
        if (supportsTransactions) g.tx().rollback();
        final GraphTraversal<Vertex,Vertex> t = g.V().has(CredentialGraphTokens.PROPERTY_USERNAME, username);
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
        if (supportsTransactions) graph.tx().rollback();

        try {
            final Vertex v =  graph.addVertex(T.label, CredentialGraphTokens.VERTEX_LABEL_USER,
                                              CredentialGraphTokens.PROPERTY_USERNAME, username,
                                              CredentialGraphTokens.PROPERTY_PASSWORD, BCrypt.hashpw(password, BCrypt.gensalt(BCRYPT_ROUNDS)));
            if (supportsTransactions) graph.tx().commit();
            return v;
        } catch (Exception ex) {
            if (supportsTransactions) graph.tx().rollback();
            throw new RuntimeException(ex);
        }
    }

    /**
     * Removes a user by name.
     *
     * @return the number of users removed (which should be one or zero)
     */
    public long removeUser(final String username) {
        if (supportsTransactions) graph.tx().rollback();
        try {
            final long count = g.V().has(CredentialGraphTokens.PROPERTY_USERNAME, username).sideEffect(drop()).count().next();
            if (supportsTransactions) graph.tx().commit();
            return count;
        } catch (Exception ex) {
            if (supportsTransactions) graph.tx().rollback();
            throw new RuntimeException(ex);
        }
    }

    /**
     * Get a count of the number of users in the database.
     */
    public long countUsers() {
        if (supportsTransactions) graph.tx().rollback();
        return g.V().hasLabel(CredentialGraphTokens.VERTEX_LABEL_USER).count().next();
    }

    @Override
    public String toString() {
        return "CredentialGraph{" +
                "graph=" + graph +
                '}';
    }

    /**
     * Wrap up any {@link Graph} instance in the {@code CredentialGraph} DSL.
     */
    public static CredentialGraph credentials(final Graph graph) {
        return new CredentialGraph(graph);
    }
}
