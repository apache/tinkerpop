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

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.auth.AllowAllAuthenticator;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.Krb5Authenticator;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.util.Log4jRecordingAppender;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.log4j.Level.INFO;
import static org.apache.tinkerpop.gremlin.server.GremlinServer.AUDIT_LOGGER_NAME;
import static org.apache.tinkerpop.gremlin.server.GremlinServerAuthKrb5IntegrateTest.TESTCONSOLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test audit logs. Like other descendants of AbstractGremlinServerIntegrationTest this test suite assumes that
 * tests are run sequentially and thus the server and kdcServer variables can be reused.
 *
 * @author Marc de Lignie
 */
public class GremlinServerAuditLogIntegrateTest extends AbstractGremlinServerIntegrationTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(GremlinServerAuditLogIntegrateTest.class);
    private Log4jRecordingAppender recordingAppender = null;

    private final ObjectMapper mapper = new ObjectMapper();
    private final Base64.Encoder encoder = Base64.getUrlEncoder();

    private static final boolean AUDIT_LOG_ENABLED = true;
    private static final boolean AUDIT_LOG_DISABLED = false;
    private static final String TESTCONSOLE2 = "GremlinConsole2";

    private KdcFixture kdcServer;

    @Override
    public void setUp() throws Exception {
        recordingAppender = new Log4jRecordingAppender();
        final Logger rootLogger = Logger.getRootLogger();
        rootLogger.addAppender(recordingAppender);

        try {
            final String moduleBaseDir = System.getProperty("basedir");
            final String authConfigName = moduleBaseDir + "/src/test/resources/org/apache/tinkerpop/gremlin/server/gremlin-console-jaas.conf";
            System.setProperty("java.security.auth.login.config", authConfigName);
            kdcServer = new KdcFixture(moduleBaseDir);
            kdcServer.setUp();
        } catch(Exception e)  {
            logger.warn(e.getMessage());
        }
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        final Logger rootLogger = Logger.getRootLogger();
        rootLogger.removeAppender(recordingAppender);
        kdcServer.close();
        System.clearProperty("java.security.auth.login.config");
        super.tearDown();
    }

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        settings.host = kdcServer.gremlinHostname;
        final Settings.SslSettings sslConfig = new Settings.SslSettings();
        sslConfig.enabled = false;
        settings.ssl = sslConfig;
        final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
        settings.authentication = authSettings;
        settings.enableAuditLog = AUDIT_LOG_ENABLED;
        authSettings.authenticator = Krb5Authenticator.class.getName();
        final Map<String,Object> authConfig = new HashMap<>();
        authSettings.config = authConfig;

        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldAuditLogWithAllowAllAuthenticator":
                authSettings.authenticator = AllowAllAuthenticator.class.getName();
                break;
            case "shouldAuditLogWithTraversalOp":
            case "shouldAuditLogWithSimpleAuthenticator":
                authSettings.authenticator = SimpleAuthenticator.class.getName();
                authConfig.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");
                break;
            case "shouldNotAuditLogWhenDisabled":
                settings.enableAuditLog = AUDIT_LOG_DISABLED;
            case "shouldAuditLogWithKrb5Authenticator":
            case "shouldAuditLogTwoClientsWithKrb5Authenticator":
                authConfig.put("keytab", kdcServer.serviceKeytabFile.getAbsolutePath());
                authConfig.put("principal", kdcServer.serverPrincipal);
                break;
            case "shouldAuditLogWithHttpTransport":
                settings.host = "localhost";
                settings.channelizer = HttpChannelizer.class.getName();
                authSettings.authenticator = SimpleAuthenticator.class.getName();
                authConfig.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");
                break;
        }
        return settings;
    }

    @Test
    public void shouldAuditLogWithAllowAllAuthenticator() throws Exception {

        final Cluster cluster = TestClientFactory.build().addContactPoint(kdcServer.gremlinHostname).create();
        final Client client = cluster.connect();

        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        stopServer();
        Thread.sleep(1000);

        // WebSocketChannelizer does not add SaslAuthenticationHandler for AllowAllAuthenticator,
        // so no authenticated user log line available
        assertTrue(recordingAppender.logMatchesAny(AUDIT_LOGGER_NAME, INFO, String.format(
                "User %s with address .+? requested: 1\\+1", AuthenticatedUser.ANONYMOUS_USERNAME)));
        assertTrue(recordingAppender.logMatchesAny(AUDIT_LOGGER_NAME, INFO, String.format(
                "User %s with address .+? requested: 1\\+2", AuthenticatedUser.ANONYMOUS_USERNAME)));
        assertTrue(recordingAppender.logMatchesAny(AUDIT_LOGGER_NAME, INFO, String.format(
                "User %s with address .+? requested: 1\\+3", AuthenticatedUser.ANONYMOUS_USERNAME)));
    }

    @Test
    public void shouldAuditLogWithSimpleAuthenticator() throws Exception {
        final String username = "stephen";
        final String password = "password";

        final Cluster cluster = TestClientFactory.build().credentials(username, password)
                .addContactPoint(kdcServer.gremlinHostname).create();
        final Client client = cluster.connect();

        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
            assertEquals(5, client.submit("1+4").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        stopServer();
        Thread.sleep(1000);

        final String simpleAuthenticatorName = SimpleAuthenticator.class.getSimpleName();

        final List<LoggingEvent> log = recordingAppender.getEvents();
        final Stream<LoggingEvent> auditEvents = log.stream().filter(event -> event.getLoggerName().equals(AUDIT_LOGGER_NAME));
        final LoggingEvent authEvent = auditEvents
                .filter(event -> event.getMessage().toString().contains(simpleAuthenticatorName)).iterator().next();
        final String authMsg = authEvent.getMessage().toString();
        assertTrue(authEvent.getLevel() == INFO &&
                authMsg.matches(String.format("User %s with address .+? authenticated by %s", username, simpleAuthenticatorName)));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User stephen with address .+? requested: 1\\+1")));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User stephen with address .+? requested: 1\\+2")));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User stephen with address .+? requested: 1\\+3")));
    }

    @Test
    public void shouldAuditLogWithKrb5Authenticator() throws Exception {
        final Cluster cluster = retryClusterBuilder(TESTCONSOLE, 10);
        final Client client = cluster.connect();
        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        stopServer();
        Thread.sleep(1000);

        final List<LoggingEvent> log = recordingAppender.getEvents();
        final Stream<LoggingEvent> auditEvents = log.stream().filter(event -> event.getLoggerName().equals(AUDIT_LOGGER_NAME));
        final LoggingEvent authEvent = auditEvents
                .filter(event -> event.getMessage().toString().contains("Krb5Authenticator")).iterator().next();
        final String authMsg = authEvent.getMessage().toString();
        assertTrue(authEvent.getLevel() == INFO &&
                authMsg.matches(String.format("User %s with address .+? authenticated by Krb5Authenticator", kdcServer.clientPrincipalName)));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User drankye with address .+? requested: 1\\+1")));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User drankye with address .+? requested: 1\\+2")));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User drankye with address .+? requested: 1\\+3")));
    }

    @Test
    public void shouldNotAuditLogWhenDisabled() throws Exception {
        final Cluster cluster = retryClusterBuilder(TESTCONSOLE, 10);
        final Client client = cluster.connect();
        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        stopServer();
        Thread.sleep(1000);

        final List<LoggingEvent> log = recordingAppender.getEvents();
        assertFalse(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User drankye with address .+? authenticated by Krb5Authenticator")));
        assertFalse(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User drankye with address .+? requested: 1\\+1")));
        assertFalse(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User drankye with address .+? requested: 1\\+2")));
        assertFalse(log.stream().anyMatch(item -> item.getLevel() == INFO &&
            item.getMessage().toString().matches("User drankye with address .+? requested: 1\\+3")));
    }

    @Test
    public void shouldAuditLogWithHttpTransport() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=2-1"));
        httpget.addHeader("Authorization", "Basic " + encoder.encodeToString("stephen:password".getBytes()));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        stopServer();
        Thread.sleep(1000);

        final String simpleAuthenticatorName = SimpleAuthenticator.class.getSimpleName();

        final List<LoggingEvent> log = recordingAppender.getEvents();
        final Stream<LoggingEvent> auditEvents = log.stream().filter(event -> event.getLoggerName().equals(AUDIT_LOGGER_NAME));
        final LoggingEvent authEvent = auditEvents
                .filter(event -> event.getMessage().toString().contains(simpleAuthenticatorName)).iterator().next();
        final String authMsg = authEvent.getMessage().toString();
        assertTrue(authEvent.getLevel() == INFO &&
                authMsg.matches(String.format("User stephen with address .+? authenticated by %s", simpleAuthenticatorName)));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User stephen with address .+? requested: 2-1")));
    }

    @Test
    public void shouldAuditLogWithTraversalOp() throws Exception {
        final String username = "stephen";
        final String password = "password";

        final Cluster cluster = TestClientFactory.build().credentials(username, password)
                .addContactPoint(kdcServer.gremlinHostname).create();
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

        final String simpleAuthenticatorName = SimpleAuthenticator.class.getSimpleName();

        final List<LoggingEvent> log = recordingAppender.getEvents();
        final Stream<LoggingEvent> auditEvents = log.stream().filter(event -> event.getLoggerName().equals(AUDIT_LOGGER_NAME));
        final LoggingEvent authEvent = auditEvents
                .filter(event -> event.getMessage().toString().contains(simpleAuthenticatorName)).iterator().next();
        final String authMsg = authEvent.getMessage().toString();
        assertTrue(authEvent.getLevel() == INFO &&
                authMsg.matches(String.format("User %s with address .+? authenticated by %s", username, simpleAuthenticatorName)));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User .+? with address .+? requested: \\[\\[], \\[V\\(\\), count\\(\\)]]")));
    }

    @Test
    public void shouldAuditLogTwoClientsWithKrb5Authenticator() throws Exception {
        final Cluster cluster = retryClusterBuilder(TESTCONSOLE, 10);
        final Client client = cluster.connect();
        final Cluster cluster2 = retryClusterBuilder(TESTCONSOLE2, 10);
        final Client client2 = cluster2.connect();
        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(22, client2.submit("11+11").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(23, client2.submit("11+12").all().get().get(0).getInt());
            assertEquals(24, client2.submit("11+13").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
            cluster2.close();
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        stopServer();
        Thread.sleep(1000);

        final List<LoggingEvent> log = recordingAppender.getEvents();
        final Stream<LoggingEvent> auditEvents = log.stream().filter(event -> event.getLoggerName().equals(AUDIT_LOGGER_NAME));
        final LoggingEvent authEvent = auditEvents
                .filter(event -> event.getMessage().toString().contains("Krb5Authenticator")).iterator().next();
        final String authMsg = authEvent.getMessage().toString();
        assertTrue(authEvent.getLevel() == INFO &&
                authMsg.matches(String.format("User %s with address .+? authenticated by Krb5Authenticator", kdcServer.clientPrincipalName)));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User drankye with address .+? requested: 1\\+1")));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User drankye with address .+? requested: 1\\+2")));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User drankye with address .+? requested: 1\\+3")));

        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches(String.format("User %s with address .+? authenticated by Krb5Authenticator", kdcServer.clientPrincipalName2))));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User drankye2 with address .+? requested: 11\\+11")));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User drankye2 with address .+? requested: 11\\+12")));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User drankye2 with address .+? requested: 11\\+13")));
    }

    private Cluster retryClusterBuilder(final String jaasEntry, final int retries) throws Exception {
        int tries = 0;
        while (tries < retries) {
            try {
                return TestClientFactory.build().jaasEntry(jaasEntry).
                        protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.gremlinHostname).create();
            } catch (Exception ex) {
                if (!ex.getMessage().equals("Authenticator is not ready to handle requests")) {
                    // got an error that may not mean retry so throw
                    throw ex;
                }

                // make the test wait a bit for the server to get kerberos settled
                Thread.sleep(tries * 3000);

                tries++;
                logger.warn("Gremlin Server authenticator is not ready - retrying Cluster creation: {} of {} tries",
                        tries, retries);
            }
        }

        throw new IllegalStateException(String.format(
                "After %s retries the Gremlin Server authenticator never became ready", retries));
    }
}