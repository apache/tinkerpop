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
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import static org.apache.log4j.Level.INFO;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Channelizer;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.NioChannelizer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.server.auth.AllowAllAuthenticator;
import org.apache.tinkerpop.gremlin.server.auth.Krb5Authenticator;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import static org.apache.tinkerpop.gremlin.server.GremlinServerAuthKrb5IntegrateTest.TESTCONSOLE;
import static org.apache.tinkerpop.gremlin.server.GremlinServer.AUDIT_LOGGER_NAME;
import org.apache.tinkerpop.gremlin.util.Log4jRecordingAppender;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test audit logs  and like other descendants of AbstractGremlinServerIntegrationTest this test suite assumes that
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

    @Before
    @Override
    public void setUp() throws Exception {
        try {
            final String buildDir = System.getProperty("build.dir");
            kdcServer = new KdcFixture(buildDir +
                    "/test-classes/org/apache/tinkerpop/gremlin/server/gremlin-console-jaas.conf");
            kdcServer.setUp();
        } catch(Exception e)  {
            logger.warn(e.getMessage());
        }
        super.setUp();
    }

    @Before
    public void setupForEachTest() {
        recordingAppender = new Log4jRecordingAppender();
        final Logger rootLogger = Logger.getRootLogger();
        rootLogger.addAppender(recordingAppender);
    }

    @After
    public void teardownForEachTest() throws Exception {
        final Logger rootLogger = Logger.getRootLogger();
        rootLogger.removeAppender(recordingAppender);
        kdcServer.close();
    }

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        settings.host = kdcServer.hostname;
        final Settings.SslSettings sslConfig = new Settings.SslSettings();
        sslConfig.enabled = false;
        settings.ssl = sslConfig;
        final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
        settings.authentication = authSettings;
        authSettings.enableAuditLog = AUDIT_LOG_ENABLED;
        authSettings.authenticator = Krb5Authenticator.class.getName();
        final Map<String,Object> authConfig = new HashMap<>();
        authSettings.config = authConfig;

        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldAuditLogWithAllowAllAuthenticator":
                authSettings.authenticator = AllowAllAuthenticator.class.getName();
                break;
            case "shouldAuditLogWithSimpleAuthenticator":
                authSettings.authenticator = SimpleAuthenticator.class.getName();
                authConfig.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");
                break;
            case "shouldNotAuditLogWhenDisabled":
                authSettings.enableAuditLog = AUDIT_LOG_DISABLED;
            case "shouldAuditLogWithKrb5Authenticator":
            case "shouldAuditLogTwoClientsWithKrb5Authenticator":
                authConfig.put("keytab", kdcServer.serviceKeytabFile.getAbsolutePath());
                authConfig.put("principal", kdcServer.serverPrincipal);
                break;
            case "shouldAuditLogWithNioTransport":
                settings.channelizer = NioChannelizer.class.getName();
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

        final Cluster cluster = TestClientFactory.build().addContactPoint(kdcServer.hostname).create();
        final Client client = cluster.connect();

        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        Thread.sleep(1000);

        // WebSocketChannelizer does not add SaslAuthenticationHandler for AllowAllAuthenticator,
        // so no authenticated user log line available
        assertTrue(recordingAppender.logMatchesAny(AUDIT_LOGGER_NAME, INFO, "User with address .*? requested: 1\\+1"));
        assertTrue(recordingAppender.logMatchesAny(AUDIT_LOGGER_NAME, INFO, "User with address .*? requested: 1\\+2"));
        assertTrue(recordingAppender.logMatchesAny(AUDIT_LOGGER_NAME, INFO, "User with address .*? requested: 1\\+3"));
    }

    @Test
    public void shouldAuditLogWithSimpleAuthenticator() throws Exception {
        final String username = "stephen";
        final String password = "password";

        final Cluster cluster = TestClientFactory.build().credentials(username, password)
                .addContactPoint(kdcServer.hostname).create();
        final Client client = cluster.connect();

        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
            assertEquals(5, client.submit("1+4").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }

        final String simpleAuthenticatorName = SimpleAuthenticator.class.getSimpleName();
        final String authMsg = recordingAppender.getMessages().stream()
                .filter(msg -> msg.contains("by " + simpleAuthenticatorName)).iterator().next();
        final Matcher m = Pattern.compile(".*?([\\d.:]+).*?").matcher(authMsg);
        m.find();
        final String address = m.group(1);

        // wait for logger to flush - (don't think there is a way to detect this)
        Thread.sleep(1000);

        assertTrue(recordingAppender.logContainsAny(AUDIT_LOGGER_NAME, INFO,
                String.format("User %s with address %s authenticated by %s", username, address, simpleAuthenticatorName)));
        assertTrue(recordingAppender.logContainsAny(AUDIT_LOGGER_NAME, INFO,
                String.format("User with address %s requested: 1+1", address)));
        assertTrue(recordingAppender.logContainsAny(AUDIT_LOGGER_NAME, INFO,
                String.format("User with address %s requested: 1+2", address)));
        assertTrue(recordingAppender.logContainsAny(AUDIT_LOGGER_NAME, INFO,
                String.format("User with address %s requested: 1+3", address)));
    }

    @Test
    public void shouldAuditLogWithKrb5Authenticator() throws Exception {
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).create();
        final Client client = cluster.connect();
        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        Thread.sleep(1000);

        final List<LoggingEvent> log = recordingAppender.getEvents();
        final Stream<LoggingEvent> auditEvents = log.stream().filter(event -> event.getLoggerName().equals(AUDIT_LOGGER_NAME));
        final LoggingEvent authEvent = auditEvents
                .filter(event -> event.getMessage().toString().contains("Krb5Authenticator")).iterator().next();
        final String authMsg = authEvent.getMessage().toString();
        final Matcher m = Pattern.compile(".*?([\\d.:]+).*?").matcher(authMsg);
        m.find();
        final String address = m.group(1);
        assertTrue(authEvent.getLevel() == INFO && authMsg.equals(
                String.format("User %s with address %s authenticated by Krb5Authenticator", kdcServer.clientPrincipalName, address)));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().equals(String.format("User with address %s requested: 1+1", address))));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().equals(String.format("User with address %s requested: 1+2", address))));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().equals(String.format("User with address %s requested: 1+3", address))));
    }

    @Test
    public void shouldNotAuditLogWhenDisabled() throws Exception {
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).create();
        final Client client = cluster.connect();
        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        Thread.sleep(1000);

        final List<LoggingEvent> log = recordingAppender.getEvents();
        assertFalse(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User .*? with address .*? authenticated by Krb5Authenticator")));
        assertFalse(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User with address .*? requested: 1\\+1")));
        assertFalse(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().matches("User with address .*? requested: 1\\+2")));
        assertFalse(log.stream().anyMatch(item -> item.getLevel() == INFO &&
            item.getMessage().toString().matches("User with address .*? requested: 1\\+3")));
    }

    @Test
    public void shouldAuditLogWithNioTransport() throws Exception {
        final Cluster cluster = TestClientFactory.build().channelizer(Channelizer.NioChannelizer.class.getName())
                .jaasEntry(TESTCONSOLE).protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).create();
        final Client client = cluster.connect();
        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        Thread.sleep(1000);

        final List<LoggingEvent> log = recordingAppender.getEvents();
        final Stream<LoggingEvent> auditEvents = log.stream().filter(event -> event.getLoggerName().equals(AUDIT_LOGGER_NAME));
        final LoggingEvent authEvent = auditEvents
                .filter(event -> event.getMessage().toString().contains("Krb5Authenticator")).iterator().next();
        final String authMsg = authEvent.getMessage().toString();
        final Matcher m = Pattern.compile(".*?([\\d.:]+).*?").matcher(authMsg);
        m.find();
        final String address = m.group(1);
        assertTrue(authEvent.getLevel() == INFO && authMsg.equals(
                String.format("User %s with address %s authenticated by Krb5Authenticator", kdcServer.clientPrincipalName, address)));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().equals(String.format("User with address %s requested: 1+1", address))));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().equals(String.format("User with address %s requested: 1+2", address))));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().equals(String.format("User with address %s requested: 1+3", address))));
    }

    @Test
    public void shouldAuditLogWithHttpTransport() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=1-1"));
        httpget.addHeader("Authorization", "Basic " + encoder.encodeToString("stephen:password".getBytes()));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).intValue());
        }

        // wait for logger to flush - (don't think there is a way to detect this)
        Thread.sleep(1000);

        final List<LoggingEvent> log = recordingAppender.getEvents();
        final Stream<LoggingEvent> auditEvents = log.stream().filter(event -> event.getLoggerName().equals(AUDIT_LOGGER_NAME));
        final LoggingEvent authEvent = auditEvents
                .filter(event -> event.getMessage().toString().contains("SimpleAuthenticator")).iterator().next();
        final String authMsg = authEvent.getMessage().toString();
        final Matcher m = Pattern.compile(".*?([\\d.:]+).*?").matcher(authMsg);
        m.find();
        final String address = m.group(1);
        assertTrue(authEvent.getLevel() == INFO && authMsg.equals(
                String.format("User %s with address %s authenticated by SimpleAuthenticator", "stephen", address)));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().equals(String.format("User with address %s requested: 1-1", address))));
    }

    @Test
    public void shouldAuditLogTwoClientsWithKrb5Authenticator() throws Exception {
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).create();
        final Client client = cluster.connect();
        final Cluster cluster2 = TestClientFactory.build().jaasEntry(TESTCONSOLE2)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).create();
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
        Thread.sleep(1000);

        final List<LoggingEvent> log = recordingAppender.getEvents();
        final Stream<LoggingEvent> auditEvents = log.stream().filter(event -> event.getLoggerName().equals(AUDIT_LOGGER_NAME));
        final Iterator<LoggingEvent> authEvents = auditEvents
                .filter(event -> event.getMessage().toString().contains("Krb5Authenticator")).iterator();

        // First client
        final LoggingEvent authEvent = authEvents.next();
        final String authMsg = authEvent.getMessage().toString();
        final Matcher m = Pattern.compile(".*?([\\d.:]{13,21}).*?").matcher(authMsg);
        m.find();
        final String address = m.group(1);
        assertTrue(authEvent.getLevel() == INFO && authMsg.equals(
                String.format("User %s with address %s authenticated by Krb5Authenticator", kdcServer.clientPrincipalName, address)));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().equals(String.format("User with address %s requested: 1+1", address))));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().equals(String.format("User with address %s requested: 1+2", address))));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().equals(String.format("User with address %s requested: 1+3", address))));

        // Second client
        final LoggingEvent authEvent2 = authEvents.next();
        final String authMsg2 = authEvent2.getMessage().toString();
        final Matcher m2 = Pattern.compile(".*?([\\d.:]{13,21}).*?").matcher(authMsg2);
        m2.find();
        final String address2 = m2.group(1);
        assertTrue(authEvent2.getLevel() == INFO && authMsg2.equals(
                String.format("User %s with address %s authenticated by Krb5Authenticator", kdcServer.clientPrincipalName2, address2)));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().equals(String.format("User with address %s requested: 11+11", address2))));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().equals(String.format("User with address %s requested: 11+12", address2))));
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == INFO &&
                item.getMessage().toString().equals(String.format("User with address %s requested: 11+13", address2))));
    }
}