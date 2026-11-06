/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server;

import nl.altindag.log.LogCaptor;
import org.apache.http.Consts;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.auth.AllowAllAuthenticator;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.server.handler.HttpBasicAuthenticationHandler;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.driver.auth.Auth.basic;
import static org.apache.tinkerpop.gremlin.server.GremlinServer.AUDIT_LOGGER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test audit logs. Like other descendants of AbstractGremlinServerIntegrationTest this test suite assumes that
 * tests are run sequentially and thus the server variable can be reused.
 *
 * @author Marc de Lignie
 */
public class GremlinServerAuditLogIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private static LogCaptor logCaptor;

    private final ObjectMapper mapper = new ObjectMapper();
    private final Base64.Encoder encoder = Base64.getUrlEncoder();

    private static final boolean AUDIT_LOG_ENABLED = true;
    private static final boolean AUDIT_LOG_DISABLED = false;
    private static final String TESTCONSOLE2 = "GremlinConsole2";

    @BeforeClass
    public static void setupLogCaptor() {
        logCaptor = LogCaptor.forName(AUDIT_LOGGER_NAME);
    }

    @AfterClass
    public static void tearDownAfterClass() {
        logCaptor.close();
    }

    @Before
    public void setupForEachTest() {
        logCaptor.clearLogs();
    }

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        settings.host = "localhost";
        final Settings.SslSettings sslConfig = new Settings.SslSettings();
        sslConfig.enabled = false;
        settings.ssl = sslConfig;
        final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
        settings.authentication = authSettings;
        settings.enableAuditLog = AUDIT_LOG_ENABLED;
        authSettings.authenticator = SimpleAuthenticator.class.getName();
        final Map<String,Object> authConfig = new HashMap<>();
        authSettings.config = authConfig;
        authConfig.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");

        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldAuditLogWithAllowAllAuthenticator":
                authSettings.authenticator = AllowAllAuthenticator.class.getName();
                authConfig.remove(SimpleAuthenticator.CONFIG_CREDENTIALS_DB);
                break;
            case "shouldNotAuditLogWhenDisabled":
                settings.enableAuditLog = AUDIT_LOG_DISABLED;
                break;
            case "shouldAuditLogWithHttpTransport":
                settings.channelizer = HttpChannelizer.class.getName();
                authSettings.authenticationHandler = HttpBasicAuthenticationHandler.class.getName();
                break;
        }
        return settings;
    }

    @Test
    public void shouldAuditLogWithAllowAllAuthenticator() throws Exception {

        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
            assertEquals(3, client.submit("g.inject(3)").all().get().get(0).getInt());
            assertEquals(4, client.submit("g.inject(4)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        stopServer();
        Thread.sleep(1000);

        // WebSocketChannelizer does not add SaslAuthenticationHandler for AllowAllAuthenticator,
        // so no authenticated user log line available
        assertTrue(logCaptor.getLogs().stream().anyMatch(m -> m.matches(String.format(
                "User %s with address .+? requested: g.inject\\(2\\)", AuthenticatedUser.ANONYMOUS_USERNAME))));
        assertTrue(logCaptor.getLogs().stream().anyMatch(m -> m.matches(String.format(
                "User %s with address .+? requested: g.inject\\(3\\)", AuthenticatedUser.ANONYMOUS_USERNAME))));
        assertTrue(logCaptor.getLogs().stream().anyMatch(m -> m.matches(String.format(
                "User %s with address .+? requested: g.inject\\(4\\)", AuthenticatedUser.ANONYMOUS_USERNAME))));
    }

    @Test
    public void shouldAuditLogWithSimpleAuthenticator() throws Exception {
        final String username = "stephen";
        final String password = "password";

        final Cluster cluster = TestClientFactory.build().auth(basic(username, password)).create();
        final Client client = cluster.connect();

        try {
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
            assertEquals(3, client.submit("g.inject(3)").all().get().get(0).getInt());
            assertEquals(4, client.submit("g.inject(4)").all().get().get(0).getInt());
            assertEquals(5, client.submit("g.inject(5)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        stopServer();
        Thread.sleep(1000);

        final String simpleAuthenticatorName = SimpleAuthenticator.class.getSimpleName();
        assertTrue(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                String.format("User %s with address .+? authenticated by %s", username, simpleAuthenticatorName))));
        assertTrue(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                "User stephen with address .+? requested: g.inject\\(2\\)")));
        assertTrue(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                "User stephen with address .+? requested: g.inject\\(3\\)")));
        assertTrue(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                "User stephen with address .+? requested: g.inject\\(4\\)")));
    }

    @Test
    public void shouldNotAuditLogWhenDisabled() throws Exception {
        final String username = "stephen";
        final String password = "password";

        final Cluster cluster = TestClientFactory.build().auth(basic(username, password)).create();
        final Client client = cluster.connect();
        try {
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
            assertEquals(3, client.submit("g.inject(3)").all().get().get(0).getInt());
            assertEquals(4, client.submit("g.inject(4)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        stopServer();
        Thread.sleep(1000);

        final String simpleAuthenticatorName = SimpleAuthenticator.class.getSimpleName();
        assertFalse(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                String.format("User %s with address .+? authenticated by %s", username, simpleAuthenticatorName))));
        assertFalse(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                "User stephen with address .+? requested: g.inject\\(2\\)")));
        assertFalse(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                "User stephen with address .+? requested: g.inject\\(3\\)")));
        assertFalse(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                "User stephen with address .+? requested: g.inject\\(4\\)")));
    }

    @Test
    public void shouldAuditLogWithHttpTransport() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httpPost = new HttpPost(TestClientFactory.createURLString());
        httpPost.addHeader("Authorization", "Basic " + encoder.encodeToString("stephen:password".getBytes()));
        httpPost.setEntity(new StringEntity("{\"gremlin\":\"g.inject(1)\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httpPost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1, node.get("result").get(SerTokens.TOKEN_DATA).get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        stopServer();
        Thread.sleep(1000);

        final String authenticatorName = SimpleAuthenticator.class.getSimpleName();
        assertTrue(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                String.format("User stephen with address .+? authenticated by %s", authenticatorName))));
        assertTrue(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                "User stephen with address .+? requested: g.inject\\(1\\)")));
    }

    @Test
    public void shouldAuditLogWithTraversalOp() throws Exception {
        final String username = "stephen";
        final String password = "password";

        final Cluster cluster = TestClientFactory.build().auth(basic(username, password)).create();
        final GraphTraversalSource g = AnonymousTraversalSource.traversal().
                withRemote(DriverRemoteConnection.using(cluster, "gmodern"));

        try {
            assertEquals(6, g.V().count().next().intValue());
        } finally {
            cluster.close();
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        stopServer();
        Thread.sleep(1000);

        final String authenticatorName = SimpleAuthenticator.class.getSimpleName();
        assertTrue(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                String.format("User %s with address .+? authenticated by %s", username, authenticatorName))));
        assertTrue(logCaptor.getLogs().stream().anyMatch(m ->
                m.matches("User .+? with address .+? requested: g.V\\(\\).count\\(\\)")));
    }
}