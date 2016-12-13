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
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.ietf.jgss.GSSException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;

import java.io.File;
import java.util.*;
import javax.security.auth.login.LoginException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;



/**
 * @author Marc de Lignie
 *
 * Todo: change back to integrate test later on
 */
public class AuthKrb5Test extends AuthKrb5TestBase {
    // Cannot use mere slf4j because need to check log output
    private final Logger rootLogger = Logger.getRootLogger();
    private final Logger logger = Logger.getLogger(AuthKrb5Test.class);

    private GremlinServerAuthKrb5Integrate server;
    private static final String TESTCONSOLE = "GremlinConsole";
    private static final String TESTCONSOLE_NOT_LOGGED_IN = "UserNotLoggedIn";

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
            .protocol(getServerPrincipalName()).addContactPoint(getHostname()).create();
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
        final Cluster cluster = TestClientFactory.build().protocol(getServerPrincipalName())
                .addContactPoint(getHostname()).create();
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
                .protocol(getServerPrincipalName()).addContactPoint(getHostname()).create();
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

    @Test
    public void shouldFailWithNonexistentServerPrincipal() throws Exception {
        final TestAppender appender = new TestAppender();
        rootLogger.addAppender(appender);
        server = new GremlinServerAuthKrb5Integrate("no-service", serviceKeytabFile);
        server.setUp();
        server.tearDown();

        final List<LoggingEvent> log = appender.getLog();
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == Level.WARN &&
                item.getMessage() == "Failed to login to kdc"));
    }

    class TestAppender extends AppenderSkeleton {
        private final List<LoggingEvent> log = new ArrayList<LoggingEvent>();

        @Override
        public boolean requiresLayout() {
            return false;
        }

        protected void append(final LoggingEvent loggingEvent) {
            log.add(loggingEvent);
        }

        @Override
        public void close() {
        }

        public List<LoggingEvent> getLog() {
            return new ArrayList<LoggingEvent>(log);
        }
    }

    @Test
    public void shouldFailWithEmptyServerKeytab() throws Exception {
        final TestAppender appender = new TestAppender();
        rootLogger.addAppender(appender);
        final File keytabFile = new File(".", "no-file");
        server = new GremlinServerAuthKrb5Integrate(getServerPrincipal(), keytabFile);
        server.setUp();
        server.tearDown();

        final List<LoggingEvent> log = appender.getLog();
        for (LoggingEvent event: log) {
            System.out.println("Log: " + event.getLevel() + " " + event.getMessage());
        }
        assertTrue(log.stream().anyMatch(item -> item.getLevel() == Level.WARN &&
                item.getMessage() == "Failed to login to kdc"));
    }

    @Test
    public void shouldFailWithWrongServerKeytab() throws Exception {
        final TestAppender appender = new TestAppender();
        rootLogger.addAppender(appender);
        final String principal = "no-principal/somehost@TEST.COM";
        getKdcServer().createPrincipal(principal);
        server = new GremlinServerAuthKrb5Integrate(principal, serviceKeytabFile);
        server.setUp();
        server.tearDown();
    }

    @Test
    public void shouldAuthenticateWithQop() throws Exception {
        server = new GremlinServerAuthKrb5Integrate(getServerPrincipal(), serviceKeytabFile);
        server.setUp();
        final String oldQop = System.getProperty("javax.security.sasl.qop", "");
        System.setProperty("javax.security.sasl.qop", "auth-conf");
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
                .protocol(getServerPrincipalName()).addContactPoint(getHostname()).create();
        final Client client = cluster.connect();
        try {
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
        } finally {
            cluster.close();
            server.tearDown();
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
