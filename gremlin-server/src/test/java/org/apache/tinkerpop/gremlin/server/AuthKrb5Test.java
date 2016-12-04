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
import org.apache.kerby.kerberos.kerb.server.LoginTestBase;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.server.auth.Krb5Authenticator;
import org.ietf.jgss.GSSException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;

import java.io.File;
import java.net.Inet4Address;
import java.util.*;

import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * @author Marc de Lignie
 *
 * Todo: change back to integrate test later on
 */
public class AuthKrb5Test extends LoginTestBase {
    private static final Logger logger = LoggerFactory.getLogger(AuthKrb5Test.class);
    private GremlinServerAuthKrb5Integrate server;
    private static final String TESTCONSOLE = "GremlinConsole";
    private static final String TESTCONSOLE_NOT_LOGGED_IN = "UserNotLoggedIn";

    @Rule
    public TestName name = new TestName();

    private class GremlinServerAuthKrb5Integrate extends AbstractGremlinServerIntegrationTest {

        final String principal;
        final File keytabFile;

        GremlinServerAuthKrb5Integrate(String principal, File keytabFile) {

            this.principal = principal;
            this.keytabFile =keytabFile;
        }

        @Override
        public Settings overrideSettings(final Settings settings) {
            settings.host = hostname;
            logger.debug("Hostname: " + settings.host);
            final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
            authSettings.className = Krb5Authenticator.class.getName();
            final Map authConfig = new HashMap<String,Object>();
            authConfig.put("keytab", keytabFile.getAbsolutePath());
            authConfig.put("principal", principal);
            authSettings.config = authConfig;
            settings.authentication = authSettings;
            final Settings.SslSettings sslConfig = new Settings.SslSettings();
            sslConfig.enabled = false;
            settings.ssl = sslConfig;

//            switch (nameOfTest) {
//                case "shouldAuthenticateOverSslWithKerberosTicket":
//                case "shouldFailIfSslEnabledOnServerButNotClient":
//                    final Settings.SslSettings sslConfig = new Settings.SslSettings();
//                    sslConfig.enabled = true;
//                    settings.ssl = sslConfig;
//                    break;
//            }
            return settings;
        }
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loginServiceUsingKeytab();
        loginClientUsingTicketCache();
        final String buildDir = System.getProperty("build.dir");
        System.setProperty("java.security.auth.login.config", buildDir +
            "/test-classes/org/apache/tinkerpop/gremlin/server/gremlin-console-jaas.conf");
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
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
        server = new GremlinServerAuthKrb5Integrate(getServerPrincipal(), serviceKeytabFile);
        server.setUp();
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
            .protocol(getServerPrincipalName()).addContactPoint(hostname).create();
        final Client client = cluster.connect();
        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
            server.tearDown();
        }
    }

    @Test
    public void shouldFailWithoutClientJaasEntry() throws Exception {
        server = new GremlinServerAuthKrb5Integrate(getServerPrincipal(), serviceKeytabFile);
        server.setUp();
        final Cluster cluster = TestClientFactory.build().protocol(getServerPrincipalName()).addContactPoint(hostname).create();
        final Client client = cluster.connect();
        try {
            client.submit("1+1").all().get();
            fail("This should not succeed as the client config does not contain a JaasEntry");
        } catch(Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals(GSSException.class, root.getClass());
        } finally {
            cluster.close();
            server.tearDown();
        }
    }

    @Test
    public void shouldFailWithoutClientTicketCache() throws Exception {
        server = new GremlinServerAuthKrb5Integrate(getServerPrincipal(), serviceKeytabFile);
        server.setUp();
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE_NOT_LOGGED_IN)
                .protocol(getServerPrincipalName()).addContactPoint(hostname).create();
        final Client client = cluster.connect();
        try {
            client.submit("1+1").all().get();
            fail("This should not succeed as the client config does not contain a valid ticket cache");
        } catch(Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals(LoginException.class, root.getClass());
        } finally {
            cluster.close();
            server.tearDown();
        }
    }

//    @Test
//    public void shouldFailWithoutServerPrincipal() throws Exception {
//        String protocol = "no-service";
//        server = new GremlinServerAuthKrb5Integrate(protocol, serviceKeytabFile);
//        server.setUp();
//        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
//                .protocol(protocol).addContactPoint(hostname).create();
//        final Client client = cluster.connect();
//        try {
//            client.submit("1+1").all().get();
//            fail("This should not succeed as the client config does not contain a JaasEntry");
//        } catch(Exception ex) {
//            final Throwable root = ExceptionUtils.getRootCause(ex);
//            assertEquals(GSSException.class, root.getClass());
//        } finally {
//            cluster.close();
//            server.tearDown();
//        }
//    }

//    @Test
//    public void shouldFailWithoutServerKeytab() throws Exception {
//        server = new GremlinServerAuthKrb5Integrate(getServerPrincipal(), serviceKeytabFile);
//        server.setUp();
//    }

//    @Test
//    public void shouldFailWithAuthIdUnequalAuthzId() throws Exception {
//        server = new GremlinServerAuthKrb5Integrate(getServerPrincipal(), serviceKeytabFile);
//        server.setUp();
//    }

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

    // ToDo: test with System.setProperty("javax.security.sasl.qop", "auth-conf");
}
