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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.server.auth.Krb5Authenticator;
import org.apache.tinkerpop.gremlin.util.Log4jRecordingAppender;
import org.ietf.jgss.GSSException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.LoginException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;



/**
 * @author Marc de Lignie
 *
 * Todo: change back to integrate test later on
 */
public class AuthKrb5Test extends AbstractGremlinServerIntegrationTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(AuthKrb5Test.class);
    private Log4jRecordingAppender recordingAppender = null;

    static final String TESTCONSOLE = "GremlinConsole";
    static final String TESTCONSOLE_NOT_LOGGED_IN = "UserNotLoggedIn";

    private KdcFixture kdcServer;

    @Before
    @Override
    public void setUp() throws Exception {
        setupForEachTest();
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
        authSettings.className = Krb5Authenticator.class.getName();
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
            case "shouldAuthenticateWithQop":
                break;
        }
        return settings;
    }

    /**
     * Defaults from TestClientFactory:
     * private int port = 45940;
     * private MessageSerializer serializer = Serializers.GRYO_V1D0.simpleInstance();
     * private int nioPoolSize = Runtime.getRuntime().availableProcessors();
     * private int workerPoolSize = Runtime.getRuntime().availableProcessors() * 2;
     * private int minConnectionPoolSize = ConnectionPool.MIN_POOL_SIZE;
     * private int maxConnectionPoolSize = ConnectionPool.MAX_POOL_SIZE;
     * private int minSimultaneousUsagePerConnection = ConnectionPool.MIN_SIMULTANEOUS_USAGE_PER_CONNECTION;
     * private int maxSimultaneousUsagePerConnection = ConnectionPool.MAX_SIMULTANEOUS_USAGE_PER_CONNECTION;
     * private int maxInProcessPerConnection = Connection.MAX_IN_PROCESS;
     * private int minInProcessPerConnection = Connection.MIN_IN_PROCESS;
     * private int maxWaitForConnection = Connection.MAX_WAIT_FOR_CONNECTION;
     * private int maxWaitForSessionClose = Connection.MAX_WAIT_FOR_SESSION_CLOSE;
     * private int maxContentLength = Connection.MAX_CONTENT_LENGTH;
     * private int reconnectInitialDelay = Connection.RECONNECT_INITIAL_DELAY;
     * private int reconnectInterval = Connection.RECONNECT_INTERVAL;
     * private int resultIterationBatchSize = Connection.RESULT_ITERATION_BATCH_SIZE;
     * private long keepAliveInterval = Connection.KEEP_ALIVE_INTERVAL;
     * private String channelizer = Channelizer.WebSocketChannelizer.class.getName();
     * private boolean enableSsl = false;
     * private String trustCertChainFile = null;
     * private String keyCertChainFile = null;
     * private String keyFile = null;
     * private String keyPassword = null;
     * private SslContext sslContext = null;
     * private LoadBalancingStrategy loadBalancingStrategy = new LoadBalancingStrategy.RoundRobin();
     * private AuthProperties authProps = new AuthProperties();
     */
    @Test
    public void shouldAuthenticateWithDefaults() throws Exception {
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
    }

    @Test
    public void shouldFailWithoutClientJaasEntry() throws Exception {
        final Cluster cluster = TestClientFactory.build().protocol(kdcServer.serverPrincipalName)
                .addContactPoint(kdcServer.hostname).create();
        final Client client = cluster.connect();
        try {
            client.submit("1+1").all().get();
            fail("This should not succeed as the client config does not contain a JaasEntry");
        } catch(Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals(GSSException.class, root.getClass());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailWithoutClientTicketCache() throws Exception {
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE_NOT_LOGGED_IN)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).create();
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
        assertTrue(recordingAppender.logContainsAny("WARN - Failed to login to kdc"));
    }

    @Test
    public void shouldFailWithEmptyServerKeytab() throws Exception {
        assertTrue(recordingAppender.logContainsAny("WARN - Failed to login to kdc"));
    }

    @Test
    public void shouldFailWithWrongServerKeytab() throws Exception {
        assertTrue(recordingAppender.logContainsAny("WARN - Failed to login to kdc"));
    }

    @Test
    public void shouldAuthenticateWithQop() throws Exception {
        final String oldQop = System.getProperty("javax.security.sasl.qop", "");
        System.setProperty("javax.security.sasl.qop", "auth-conf");
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).create();
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

//    ToDo: Unclear how to test this
//    @Test
//    public void shouldFailWithAuthIdUnequalAuthzId() throws Exception {
//        server = new GremlinServerAuthKrb5Integrate(getServerPrincipal(), serviceKeytabFile);
//        server.setUp();
//    }


//    ToDo: make this into a test for gremlin-driver
//    @Test
//    public void shouldAuthenticateWithSerializeResultToString() throws Exception {
//        server = new GremlinServerAuthKrb5Integrate(getServerPrincipal(), serviceKeytabFile);
//        server.setUp();
//        loginServiceUsingKeytab();
//        loginClientUsingTicketCache();
//        MessageSerializer serializer = new GryoMessageSerializerV1d0();
//        Map config = new HashMap<String, Object>();
//        config.put("serializeResultToString", true);
//        serializer.configure(config, null);
//        final Cluster cluster = Cluster.build().jaasEntry(TESTCONSOLE)
//                .protocol(getServerPrincipalName()).addContactPoint(serverHostname).port(45940).enableSsl(false).serializer(serializer).create();
//        final Client client = cluster.connect();
//        try {
//            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
//            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
//            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
//        } finally {
//            cluster.close();
//        }
//    }
}
