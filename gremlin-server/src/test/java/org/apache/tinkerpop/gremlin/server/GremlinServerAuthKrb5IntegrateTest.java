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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Level;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.server.auth.Krb5Authenticator;
import org.ietf.jgss.GSSException;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.security.auth.login.LoginException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Marc de Lignie
 */
public class GremlinServerAuthKrb5IntegrateTest extends AbstractGremlinServerIntegrationTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(GremlinServerAuthKrb5IntegrateTest.class);

    static final String TESTCONSOLE = "GremlinConsole";
    static final String TESTCONSOLE_NOT_LOGGED_IN = "UserNotLoggedIn";

    private KdcFixture kdcServer;
    private Level previousLogLevel;

    @Override
    public void setUp() throws Exception {
        // this logger is noisy for travis and we don't assert anything and the error is already tracked on
        // the server so we can trim the logs a bit with this.
        final org.apache.log4j.Logger handlerLogger = org.apache.log4j.Logger.getLogger(
                "org.apache.tinkerpop.gremlin.driver.Handler$GremlinResponseHandler");
        previousLogLevel = handlerLogger.getLevel();
        handlerLogger.setLevel(Level.OFF);

        try {
            final String projectBaseDir = System.getProperty("basedir");
            final String authConfigName = projectBaseDir + "/src/test/resources/org/apache/tinkerpop/gremlin/server/gremlin-console-jaas.conf";
            System.setProperty("java.security.auth.login.config", authConfigName);
            kdcServer = new KdcFixture(projectBaseDir);
            kdcServer.setUp();
        } catch(Exception e)  {
            logger.warn(e.getMessage());
        }
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        final org.apache.log4j.Logger handlerLogger = org.apache.log4j.Logger.getLogger(
                "org.apache.tinkerpop.gremlin.driver.Handler$GremlinResponseHandler");
        handlerLogger.setLevel(previousLogLevel);

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
        authSettings.authenticator = Krb5Authenticator.class.getName();
        final Map<String,Object> authConfig = new HashMap<>();
        authConfig.put("principal", kdcServer.serverPrincipal);
        authConfig.put("keytab", kdcServer.serviceKeytabFile.getAbsolutePath());
        authSettings.config = authConfig;

        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldAuthenticateWithDefaults":
            case "shouldFailWithoutClientJaasEntry":
            case "shouldFailWithoutClientTicketCache":
                break;
            case "shouldFailWithNonexistentServerPrincipal":
                authConfig.put("principal", "no-service");
                break;
            case "shouldFailWithEmptyServerKeytab":
                final File keytabFile = new File(".", "no-file");
                authConfig.put("keytab", keytabFile);
                break;
            case "shouldFailWithWrongServerKeytab":
                final String principal = "no-principal/somehost@TEST.COM";
                try { kdcServer.createPrincipal(principal); } catch(Exception e) {
                    logger.error("Cannot create principal in overrideSettings(): " + e.getMessage());
                };
                authConfig.put("principal", principal);
                break;
            case "shouldAuthenticateWithSsl":
                sslConfig.enabled = true;
                sslConfig.keyStore = JKS_SERVER_KEY;
                sslConfig.keyStorePassword = KEY_PASS;
                sslConfig.keyStoreType = KEYSTORE_TYPE_JKS;
                break;
            case "shouldAuthenticateWithQop":
                break;
        }
        return settings;
    }

    @Test
    public void shouldAuthenticateWithDefaults() throws Exception {
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.gremlinHostname).create();
        final Client client = cluster.connect();
        assertConnection(cluster, client);
    }

    @Test
    public void shouldFailWithoutClientJaasEntry() throws Exception {
        final Cluster cluster = TestClientFactory.build().protocol(kdcServer.serverPrincipalName)
                .addContactPoint(kdcServer.gremlinHostname).create();
        final Client client = cluster.connect();
        try {
            client.submit("1+1").all().get();
            fail("This should not succeed as the client config does not contain a JaasEntry");
        } catch(Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertTrue(root instanceof ResponseException || root instanceof GSSException);
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailWithoutClientTicketCache() throws Exception {
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE_NOT_LOGGED_IN)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.gremlinHostname).create();
        final Client client = cluster.connect();
        try {
            client.submit("1+1").all().get();
            fail("This should not succeed as the client config does not contain a valid ticket cache");
        } catch(Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals(LoginException.class, root.getClass());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailWithNonexistentServerPrincipal() throws Exception {
        assertFailedLogin();
    }

    @Test
    public void shouldFailWithEmptyServerKeytab() throws Exception {
        assertFailedLogin();
    }

    @Test
    public void shouldFailWithWrongServerKeytab() throws Exception {
        assertFailedLogin();
    }

    @Test
    public void shouldAuthenticateWithQop() throws Exception {
        final String oldQop = System.getProperty("javax.security.sasl.qop", "");
        System.setProperty("javax.security.sasl.qop", "auth-conf");
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.gremlinHostname).create();
        final Client client = cluster.connect();
        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
            System.setProperty("javax.security.sasl.qop", oldQop);
        }
    }

    @Test
    public void shouldAuthenticateWithSsl() throws Exception {
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE).enableSsl(true).sslSkipCertValidation(true)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.gremlinHostname).create();
        final Client client = cluster.connect();
        assertConnection(cluster, client);
    }

    @Test
    public void shouldAuthenticateWithSerializeResultToStringGryoV1() throws Exception {
        assertAuthViaToStringWithSpecifiedSerializer(new GryoMessageSerializerV1d0());
    }

    @Test
    public void shouldAuthenticateWithSerializeResultToStringGryoV3() throws Exception {
        assertAuthViaToStringWithSpecifiedSerializer(new GryoMessageSerializerV3d0());
    }

    @Test
    public void shouldAuthenticateWithSerializeResultToStringGraphBinaryV1() throws Exception {
        assertAuthViaToStringWithSpecifiedSerializer(new GraphBinaryMessageSerializerV1());
    }

    public void assertAuthViaToStringWithSpecifiedSerializer(final MessageSerializer<?> serializer) throws InterruptedException, ExecutionException {
        final Map<String,Object> config = new HashMap<>();
        config.put("serializeResultToString", true);
        serializer.configure(config, null);
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.gremlinHostname).serializer(serializer).create();
        final Client client = cluster.connect();
        assertConnection(cluster, client);
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

    /**
     * Tries to force the logger to flush fully or at least wait until it does.
     */
    private void assertFailedLogin() throws Exception {
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.gremlinHostname).create();
        final Client client = cluster.connect();
        try {
            client.submit("1+1").all().get();
            fail("The kerberos config is a bust so this request should fail");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.SERVER_ERROR, re.getResponseStatusCode());
            assertEquals("Authenticator is not ready to handle requests", re.getMessage());
        } finally {
            cluster.close();
        }
    }
}
