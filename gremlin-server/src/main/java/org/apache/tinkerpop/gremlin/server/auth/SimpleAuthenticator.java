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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.server.auth.groovy.plugin.CredentialGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.server.auth.groovy.plugin.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.server.auth.groovy.plugin.CredentialGraphTokens.PROPERTY_USERNAME;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SimpleAuthenticator implements Authenticator {
    private static final Logger logger = LoggerFactory.getLogger(SimpleAuthenticator.class);
    private static final byte NUL = 0;
    private CredentialGraph credentialStore;

    public static final String CONFIG_CREDENTIALS_LOCATION = "credentialsDbLocation";
    public static final String CONFIG_CREDENTIALS_DB = "credentialsDb";

    @Override
    public boolean requireAuthentication() {
        return true;
    }

    @Override
    public void setup(final Map<String,Object> config) {
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
            if (!config.containsKey(CONFIG_CREDENTIALS_LOCATION)) {
                throw new IllegalStateException(String.format(
                        "Credentials configuration for TinkerGraph missing the %s key that points to a gryo file containing credentials data", CONFIG_CREDENTIALS_LOCATION));
            }

            final TinkerGraph tinkerGraph = (TinkerGraph) graph;
            tinkerGraph.createIndex(PROPERTY_USERNAME, Vertex.class);

            final String location = (String) config.get(CONFIG_CREDENTIALS_LOCATION);
            try {
                tinkerGraph.io(IoCore.gryo()).readGraph(location);
            } catch (IOException e) {
                logger.warn("Could not read credentials graph from {} - authentication is enabled, but with an empty user database", location);
            }
        }

        credentialStore = CredentialGraph.credentials(graph);
    }

    @Override
    public SaslNegotiator newSaslNegotiator() {
        return new PlainTextSaslAuthenticator();
    }

    private AuthenticatedUser authenticate(final String username, final String password) throws AuthenticationException {
        final Vertex user;
        try {
            user = credentialStore.findUser(username);
        } catch (IllegalStateException ex) {
            // typically thrown when there are multiple users with the same name in the credential store
            logger.warn(ex.getMessage());
            throw new AuthenticationException("Username and/or password are incorrect", ex);
        } catch (Exception ex) {
            throw new AuthenticationException("Username and/or password are incorrect", ex);
        }

        if (null == user)  throw new AuthenticationException("Username and/or password are incorrect");

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
            return authenticate(username, password);
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
