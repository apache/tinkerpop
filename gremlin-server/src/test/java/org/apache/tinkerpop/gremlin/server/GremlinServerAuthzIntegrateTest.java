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
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.auth.AllowAllAuthenticator;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.server.authz.AllowListAuthorizer;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;

import java.util.HashMap;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Run with:
 * mvn verify -Dit.test=GremlinServerAuthzIntegrateTest -pl gremlin-server -DskipTests -DskipIntegrationTests=false -Dmaven.javadoc.skip=true -Dorg.slf4j.simpleLogger.defaultLogLevel=info
 *
 * @author Marc de Lignie
 */
public class GremlinServerAuthzIntegrateTest extends AbstractGremlinServerIntegrationTest {

    /**
     * Configure Gremlin Server settings per test.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
        authSettings.config = new HashMap<>();
        final Settings.AuthorizationSettings authzSettings = new Settings.AuthorizationSettings();
        authzSettings.authorizer = AllowListAuthorizer.class.getName();

        if (name.getMethodName().contains("AllowAllAuthenticator")) {
            authSettings.authenticator = AllowAllAuthenticator.class.getName();
        } else {
            authSettings.authenticator = SimpleAuthenticator.class.getName();
            // use a credentials graph with two users in it: stephen/password and marko/rainbow-dash
            authSettings.config.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");
        }

        authzSettings.config = new HashMap<>();
        final String yamlName = "org/apache/tinkerpop/gremlin/server/allow-list.yaml";
        String file = Objects.requireNonNull(getClass().getClassLoader().getResource(yamlName)).getFile();
        authzSettings.config.put(AllowListAuthorizer.KEY_AUTHORIZATION_ALLOWLIST, file);

        settings.authentication = authSettings;
        settings.authorization = authzSettings;
        return settings;
    }

    @Test
    public void shouldAuthorizeBytecodeRequest() {
        final Cluster cluster = TestClientFactory.build().credentials("stephen", "password").create();
        final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(
                DriverRemoteConnection.using(cluster, "gmodern"));

        assertEquals(6, (long) g.V().count().next());
        cluster.close();
    }

    @Test
    public void shouldAuthorizeBytecodeRequestWithLambda() {
        final Cluster cluster = TestClientFactory.build().credentials("marko", "rainbow-dash").create();
        final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(
                DriverRemoteConnection.using(cluster, "gclassic"));

        assertEquals(6, (long) g.V().count().next());
        assertEquals(6, (long) g.V().map(Lambda.function("it.get().value('name')")).count().next());
        cluster.close();
    }

    @Test
    public void shouldFailBytecodeRequestWithLambda() {
        final Cluster cluster = TestClientFactory.build().credentials("stephen", "password").create();
        try {
            final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(
                    DriverRemoteConnection.using(cluster, "gmodern"));
            g.V().map(Lambda.function("it.get().value('name')")).count().next();
            fail("Authorization for bytecode request with lambda should fail");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.UNAUTHORIZED, re.getResponseStatusCode());
            assertEquals("Failed to authorize: User not authorized for bytecode requests with lambdas on [gmodern].", re.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAuthorizeStringRequest() throws Exception {
        final Cluster cluster = TestClientFactory.build().credentials("marko", "rainbow-dash").create();
        final Client client = cluster.connect();

        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(6, client.submit("gclassic.V().count()").all().get().get(0).getInt());
            assertEquals(6, client.submit("gmodern.V().map{it.get().value('name')}.count()").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailStringRequestWithGroovyScript() {
        final Cluster cluster = TestClientFactory.build().credentials("stephen", "password").create();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all().get();
            fail("Authorization without authentication should fail");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.UNAUTHORIZED, re.getResponseStatusCode());
            assertEquals("Failed to authorize: User not authorized for string-based requests.", re.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailStringRequestWithGremlinTraversal() {
        final Cluster cluster = TestClientFactory.build().credentials("stephen", "password").create();
        final Client client = cluster.connect();

        try {
            client.submit("gmodern.V().count()").all().get();
            fail("Authorization without authentication should fail");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.UNAUTHORIZED, re.getResponseStatusCode());
            assertEquals("Failed to authorize: User not authorized for string-based requests.", re.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailBytecodeRequestWithAllowAllAuthenticator() {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(
                    DriverRemoteConnection.using(cluster, "gclassic"));
            g.V().count().next();
            fail("Authorization without authentication should fail");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.UNAUTHORIZED, re.getResponseStatusCode());
            assertEquals("Failed to authorize: User not authorized for bytecode requests on [gclassic].", re.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailStringRequestWithAllowAllAuthenticator() {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all().get();
            fail("Authorization without authentication should fail");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.UNAUTHORIZED, re.getResponseStatusCode());
            assertEquals("Failed to authorize: User not authorized for string-based requests.", re.getMessage());
        } finally {
            cluster.close();
        }
    }
}
