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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.mindrot.jbcrypt.BCrypt;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.VERTEX_LABEL_USER;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CredentialTraversalSourceDsl extends GraphTraversalSource {
    public CredentialTraversalSourceDsl(final Graph graph, final TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
    }

    public CredentialTraversalSourceDsl(final Graph graph) {
        super(graph);
    }

    /**
     * Finds all users.
     */
    public GraphTraversal<Vertex, Vertex> users() {
        return this.clone().V().hasLabel(VERTEX_LABEL_USER);
    }

    /**
     * Finds users by name.
     */
    public GraphTraversal<Vertex, Vertex> users(final String username, final String... more) {
        if (more.length == 0) {
            return this.clone().V().has(VERTEX_LABEL_USER, PROPERTY_USERNAME, username);
        }

        final int lastIndex;
        final String[] usernames = Arrays.copyOf(more, (lastIndex = more.length) + 1);
        usernames[lastIndex] = username;
        return this.clone().V().has(VERTEX_LABEL_USER, PROPERTY_USERNAME, P.within(usernames));
    }

    /**
     * Creates or updates a user.
     */
    public GraphTraversal<Vertex, Vertex> user(final String username, final String password) {
        return this.clone().V().
                has(VERTEX_LABEL_USER, PROPERTY_USERNAME, username).
                fold().
                coalesce(__.unfold(),
                         __.addV(VERTEX_LABEL_USER).property(PROPERTY_USERNAME, username)).
                property(PROPERTY_PASSWORD, BCrypt.hashpw(password, BCrypt.gensalt(CredentialTraversalDsl.BCRYPT_ROUNDS)));
    }
}
