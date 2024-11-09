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
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.simple.WebSocketClient;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.server.handler.SaslAuthenticationHandler;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.ietf.jgss.GSSException;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerAuthIntegrateTest extends AbstractGremlinServerIntegrationTest {

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
        authSettings.authenticator = SimpleAuthenticator.class.getName();

        // use a credentials graph with two users in it: stephen/password and marko/rainbow-dash
        final Map<String,Object> authConfig = new HashMap<>();
        authConfig.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");

        authSettings.config = authConfig;
        settings.authentication = authSettings;

        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldAuthenticateOverSslWithPlainText":
            case "shouldFailIfSslEnabledOnServerButNotClient":
                final Settings.SslSettings sslConfig = new Settings.SslSettings();
                sslConfig.enabled = true;
                sslConfig.keyStore = JKS_SERVER_KEY;
                sslConfig.keyStorePassword = KEY_PASS;
                settings.ssl = sslConfig;
                break;
        }

        return settings;
    }

    @Test
    public void shouldFailIfSslEnabledOnServerButNotClient() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all().get();
            fail("This should not succeed as the client did not enable SSL");
        } catch(Exception ex) {
            assertThat(ex, instanceOf(NoHostAvailableException.class));
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertThat(root, instanceOf(RuntimeException.class));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAuthenticateWithPlainText() throws Exception {
        final Cluster cluster = TestClientFactory.build().credentials("stephen", "password").create();
        final Client client = cluster.connect();

        assertConnection(cluster, client);
    }

    @Test
    public void shouldAuthenticateOverSslWithPlainText() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .enableSsl(true).sslSkipCertValidation(true)
                .credentials("stephen", "password").create();
        final Client client = cluster.connect();

        assertConnection(cluster, client);
    }

    @Test
    public void shouldFailAuthenticateWithPlainTextNoCredentials() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all().get();
            fail("This should not succeed as the client did not provide credentials");
        } catch(Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);

            // depending on the configuration of the system environment you might get either of these
            assertThat(root, anyOf(instanceOf(GSSException.class), instanceOf(ResponseException.class)));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailAuthenticateWithPlainTextBadPassword() throws Exception {
        final Cluster cluster = TestClientFactory.build().credentials("stephen", "bad").create();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all().get();
            fail("This should not succeed as the client did not provide valid credentials");
        } catch(Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertEquals(ResponseException.class, root.getClass());
            assertEquals("Username and/or password are incorrect", root.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailAuthenticateWithPlainTextBadUsername() throws Exception {
        final Cluster cluster = TestClientFactory.build().credentials("marko", "password").create();
        final Client client = cluster.connect();

        try {
            client.submit("1+1").all().get();
        } catch(Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertEquals(ResponseException.class, root.getClass());
            assertEquals("Username and/or password are incorrect", root.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailAuthenticateWithUnAuthenticatedRequestAfterMaxDeferrableDuration() throws Exception {
        try (WebSocketClient client = TestClientFactory.createWebSocketClient()) {
            // First request will initiate the authentication handshake
            // Subsequent requests will be deferred
            CompletableFuture<List<ResponseMessage>> futureOfRequestWithinAuthDuration1  = client.submitAsync("");
            CompletableFuture<List<ResponseMessage>> futureOfRequestWithinAuthDuration2  = client.submitAsync("");
            CompletableFuture<List<ResponseMessage>> futureOfRequestWithinAuthDuration3  = client.submitAsync("");

            // After the maximum allowed deferred request duration,
            // any non-authenticated request will invalidate all requests with 429 error
            CompletableFuture<List<ResponseMessage>> futureOfRequestSubmittedTooLate = CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(SaslAuthenticationHandler.MAX_REQUEST_DEFERRABLE_DURATION.plus(Duration.ofSeconds(1)).toMillis());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }).thenCompose((__) -> {
                try {
                    return client.submitAsync("");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            assertEquals(2, futureOfRequestWithinAuthDuration1.get().size());
            assertEquals(1, futureOfRequestWithinAuthDuration2.get().size());
            assertEquals(1, futureOfRequestWithinAuthDuration3.get().size());
            assertEquals(1, futureOfRequestSubmittedTooLate.get().size());

            assertEquals(ResponseStatusCode.AUTHENTICATE, futureOfRequestWithinAuthDuration1.get().get(0).getStatus().getCode());
            assertEquals(ResponseStatusCode.UNAUTHORIZED, futureOfRequestWithinAuthDuration1.get().get(1).getStatus().getCode());
            assertEquals(ResponseStatusCode.UNAUTHORIZED, futureOfRequestWithinAuthDuration2.get().get(0).getStatus().getCode());
            assertEquals(ResponseStatusCode.UNAUTHORIZED, futureOfRequestWithinAuthDuration3.get().get(0).getStatus().getCode());
            assertEquals(ResponseStatusCode.UNAUTHORIZED, futureOfRequestSubmittedTooLate.get().get(0).getStatus().getCode());
        }
    }

    @Test
    public void shouldFailAuthenticateWithIncorrectParallelRequests() throws Exception {
        try (WebSocketClient client = TestClientFactory.createWebSocketClient()) {

            CompletableFuture<List<ResponseMessage>> firstRequest = client.submitAsync("1");
            CompletableFuture<List<ResponseMessage>> secondRequest  = client.submitAsync("2");
            CompletableFuture<List<ResponseMessage>> thirdRequest  = client.submitAsync("3");

            Thread.sleep(500);

            // send some incorrect value for username password which should cause all requests to fail.
            client.submitAsync(RequestMessage.build(Tokens.OPS_AUTHENTICATION).addArg(Tokens.ARGS_SASL, "someincorrectvalue").create());

            assertEquals(2, firstRequest.get().size());
            assertEquals(1, secondRequest.get().size());
            assertEquals(1, thirdRequest.get().size());

            assertEquals(ResponseStatusCode.AUTHENTICATE, firstRequest.get().get(0).getStatus().getCode());
            assertEquals(ResponseStatusCode.UNAUTHORIZED, firstRequest.get().get(1).getStatus().getCode());
            assertEquals(ResponseStatusCode.UNAUTHORIZED, secondRequest.get().get(0).getStatus().getCode());
            assertEquals(ResponseStatusCode.UNAUTHORIZED, thirdRequest.get().get(0).getStatus().getCode());
        }
    }

    @Test
    public void shouldAuthenticateWithPlainTextOverDefaultJSONSerialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON)
                .credentials("stephen", "password").create();
        final Client client = cluster.connect();

        assertConnection(cluster, client);
    }

    @Test
    public void shouldAuthenticateWithPlainTextOverGraphSONV1Serialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V1_UNTYPED)
                .credentials("stephen", "password").create();
        final Client client = cluster.connect();

        assertConnection(cluster, client);
    }

    @Test
    public void shouldAuthenticateAndWorkWithVariablesOverDefaultJsonSerialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON)
                .credentials("stephen", "password").create();
        final Client client = cluster.connect(name.getMethodName());

        try {
            final Vertex vertex = (Vertex) client.submit("v=graph.addVertex(\"name\", \"stephen\")").all().get().get(0).getObject();
            assertEquals("stephen", vertex.value("name"));

            final Property vpName = (Property)client.submit("v.property('name')").all().get().get(0).getObject();
            assertEquals("stephen", vpName.value());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldAuthenticateAndWorkWithVariablesOverGraphSONV1Serialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V1_UNTYPED)
                .credentials("stephen", "password").create();
        final Client client = cluster.connect(name.getMethodName());

        try {
            final Map vertex = (Map) client.submit("v=graph.addVertex('name', 'stephen')").all().get().get(0).getObject();
            final Map<String, List<Map>> properties = (Map) vertex.get("properties");
            assertEquals("stephen", properties.get("name").get(0).get("value"));

            final Map vpName = (Map)client.submit("v.property('name')").all().get().get(0).getObject();
            assertEquals("stephen", vpName.get("value"));
        } finally {
            cluster.close();
        }
    }

    private static void assertConnection(final Cluster cluster, final Client client) throws InterruptedException, ExecutionException {
        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }
}
