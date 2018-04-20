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
package org.apache.tinkerpop.gremlin.server.auth;

import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialTraversal;
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialTraversalDsl;
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;

/**
 * A simple implementation of an {@link Authenticator} that uses a {@link Graph} instance as a credential store.
 * Management of the credential store can be handled through the {@link CredentialTraversalDsl} DSL.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SimpleAuthenticator implements Authenticator {
    private static final Logger logger = LoggerFactory.getLogger(SimpleAuthenticator.class);
    private static final byte NUL = 0;
    private CredentialTraversalSource credentialStore;

    /**
     * The location of the configuration file that contains the credentials database.
     */
    public static final String CONFIG_CREDENTIALS_DB = "credentialsDb";

    @Override
    public boolean requireAuthentication() {
        return true;
    }

    @Override
    public void setup(final Map<String,Object> config) {
        logger.info("Initializing authentication with the {}", SimpleAuthenticator.class.getName());

        if (null == config) {
            throw new IllegalArgumentException(String.format(
                    "Could not configure a %s - provide a 'config' in the 'authentication' settings",
                    SimpleAuthenticator.class.getName()));
        }

        if (!config.containsKey(CONFIG_CREDENTIALS_DB)) {
            throw new IllegalStateException(String.format(
                    "Credentials configuration missing the %s key that points to a graph config file", CONFIG_CREDENTIALS_DB));
        }

        final Graph graph = GraphFactory.open((String) config.get(CONFIG_CREDENTIALS_DB));

        if (graph instanceof TinkerGraph) {
            // have to create the indices because they are not stored in gryo
            final TinkerGraph tinkerGraph = (TinkerGraph) graph;
            tinkerGraph.createIndex(PROPERTY_USERNAME, Vertex.class);
        }

        credentialStore = graph.traversal(CredentialTraversalSource.class);
        logger.info("CredentialGraph initialized at {}", credentialStore);
    }

    @Override
    public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
        return new PlainTextSaslAuthenticator();
    }

    public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException {
        final Vertex user;
        if (!credentials.containsKey(PROPERTY_USERNAME)) throw new IllegalArgumentException(String.format("Credentials must contain a %s", PROPERTY_USERNAME));
        if (!credentials.containsKey(PROPERTY_PASSWORD)) throw new IllegalArgumentException(String.format("Credentials must contain a %s", PROPERTY_PASSWORD));

        final String username = credentials.get(PROPERTY_USERNAME);
        final String password = credentials.get(PROPERTY_PASSWORD);
        final CredentialTraversal<Vertex,Vertex> t = credentialStore.users(username);
        if (!t.hasNext())
            throw new AuthenticationException("Username and/or password are incorrect");

        user = t.next();
        if (t.hasNext()) {
            logger.warn("There is more than one user with the username [{}] - usernames must be unique", username);
            throw new AuthenticationException("Username and/or password are incorrect");
        }

        final String hash = user.value(PROPERTY_PASSWORD);
        if (!BCrypt.checkpw(password, hash))
            throw new AuthenticationException("Username and/or password are incorrect");

        return new AuthenticatedUser(username);
    }

    private class PlainTextSaslAuthenticator implements Authenticator.SaslNegotiator {
        private boolean complete = false;
        private String username;
        private String password;

        @Override
        public byte[] evaluateResponse(final byte[] clientResponse) throws AuthenticationException {
            decodeCredentials(clientResponse);
            complete = true;
            return null;
        }

        @Override
        public boolean isComplete() {
            return complete;
        }

        @Override
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
            if (!complete) throw new AuthenticationException("SASL negotiation not complete");
            final Map<String,String> credentials = new HashMap<>();
            credentials.put(PROPERTY_USERNAME, username);
            credentials.put(PROPERTY_PASSWORD, password);
            return authenticate(credentials);
        }

        /**
         * SASL PLAIN mechanism specifies that credentials are encoded in a
         * sequence of UTF-8 bytes, delimited by 0 (US-ASCII NUL).
         * The form is : {code}authzId<NUL>authnId<NUL>password<NUL>{code}.
         *
         * @param bytes encoded credentials string sent by the client
         */
        private void decodeCredentials(byte[] bytes) throws AuthenticationException {
            byte[] user = null;
            byte[] pass = null;
            int end = bytes.length;
            for (int i = bytes.length - 1 ; i >= 0; i--) {
                if (bytes[i] == NUL) {
                    if (pass == null)
                        pass = Arrays.copyOfRange(bytes, i + 1, end);
                    else if (user == null)
                        user = Arrays.copyOfRange(bytes, i + 1, end);
                    end = i;
                }
            }

            if (null == user) throw new AuthenticationException("Authentication ID must not be null");
            if (null == pass) throw new AuthenticationException("Password must not be null");

            username = new String(user, StandardCharsets.UTF_8);
            password = new String(pass, StandardCharsets.UTF_8);
        }
    }
}