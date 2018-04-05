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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDsl;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.mindrot.jbcrypt.BCrypt;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.VERTEX_LABEL_USER;

/**
 * A DSL for managing a "credentials graph" used by Gremlin Server for simple authentication functions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@GremlinDsl(traversalSource = "org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialTraversalSourceDsl")
public interface CredentialTraversalDsl<S,E> extends GraphTraversal.Admin<S,E> {
    static final int BCRYPT_ROUNDS = 4;

    /**
     * Finds all users.
     */
    public default GraphTraversal<S, Vertex> users() {
        return (CredentialTraversal<S, Vertex>) hasLabel(VERTEX_LABEL_USER);
    }

    /**
     * Finds users by name.
     */
    public default GraphTraversal<S, Vertex> users(final String username, final String... more) {
        if (more.length == 0) {
            return (CredentialTraversal<S, Vertex>) has(VERTEX_LABEL_USER, PROPERTY_USERNAME, username);
        }

        final int lastIndex;
        final String[] usernames = Arrays.copyOf(more, (lastIndex = more.length) + 1);
        usernames[lastIndex] = username;
        return (CredentialTraversal<S, Vertex>)has(VERTEX_LABEL_USER, PROPERTY_USERNAME, P.within(usernames));
    }

    /**
     * Creates or updates a user.
     */
    public default GraphTraversal<S, Vertex> user(final String username, final String password) {
        return has(VERTEX_LABEL_USER, PROPERTY_USERNAME, username).
               fold().
               coalesce(__.unfold(),
                        __.addV(VERTEX_LABEL_USER).property(PROPERTY_USERNAME, username)).
               property(PROPERTY_PASSWORD, BCrypt.hashpw(password, BCrypt.gensalt(CredentialTraversalDsl.BCRYPT_ROUNDS)));
    }
}
