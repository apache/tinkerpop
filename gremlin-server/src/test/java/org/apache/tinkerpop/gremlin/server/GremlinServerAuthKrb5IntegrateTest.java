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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Channelizer;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.auth.Krb5Authenticator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.RemoteGraph;
import org.ietf.jgss.GSSException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Supplier;
import javax.security.auth.login.LoginException;

import static org.apache.tinkerpop.gremlin.process.remote.RemoteConnection.GREMLIN_REMOTE_CONNECTION_CLASS;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
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

    private final Supplier<Graph> graphGetter = () -> server.getServerGremlinExecutor().getGraphManager().getGraph("graph");
    private final Configuration conf = new BaseConfiguration() {{
        setProperty(Graph.GRAPH, RemoteGraph.class.getName());
        setProperty(GREMLIN_REMOTE_CONNECTION_CLASS, DriverRemoteConnection.class.getName());
        setProperty(DriverRemoteConnection.GREMLIN_REMOTE_DRIVER_SOURCENAME, "g");
        setProperty("hidden.for.testing.only", graphGetter);
        setProperty("clusterConfiguration.port", TestClientFactory.PORT);
        setProperty("clusterConfiguration.hosts", "localhost");
    }};

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
        authSettings.authenticator = Krb5Authenticator.class.getName();
        final Map<String,Object> authConfig = new HashMap<>();
        authConfig.put("principal", kdcServer.serverPrincipal);
        authConfig.put("keytab", kdcServer.serviceKeytabFile.getAbsolutePath());
        authSettings.config = authConfig;

        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldAuthenticateWithThreads":
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
    public void shouldAuthenticateTraversalWithThreads() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .nioPoolSize(1)
                .jaasEntry(TESTCONSOLE)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "gmodern"));

        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final Callable<Long> countTraversalJob = () -> g.V().both().both().count().next();
        final List<Future<Long>> results = executor.invokeAll(Collections.nCopies(100, countTraversalJob));

        assertEquals(100, results.size());
        for (int ix = 0; ix < results.size(); ix++) {
            try {
                assertEquals(30L, results.get(ix).get(1000, TimeUnit.MILLISECONDS).longValue());
            } catch (Exception ex) {
                // failure but shouldn't have
                cluster.close();
                fail("Exception halted assertions - " + ex.getMessage());
            }
        }

        cluster.close();
    }

    @Test
    public void shouldAuthenticateScriptWithThreads() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .nioPoolSize(1)
                .jaasEntry(TESTCONSOLE)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).create();
        final Client client = cluster.connect();

        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final Callable<Long> countTraversalJob = () -> client.submit("gmodern.V().both().both().count()").all().get().get(0).getLong();
        final List<Future<Long>> results = executor.invokeAll(Collections.nCopies(100, countTraversalJob));

        assertEquals(100, results.size());
        for (int ix = 0; ix < results.size(); ix++) {
            try {
                assertEquals(30L, results.get(ix).get(1000, TimeUnit.MILLISECONDS).longValue());
            } catch (Exception ex) {
                // failure but shouldn't have
                cluster.close();
                fail("Exception halted assertions - " + ex.getMessage());
            }
        }

        cluster.close();
    }

    @Test
    public void shouldAuthenticateWithDefaults() throws Exception {
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).create();
        final Client client = cluster.connect();
        assertConnection(cluster, client);
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
            assertTrue(root instanceof ResponseException || root instanceof GSSException);
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

    @Test
    public void shouldAuthenticateWithSsl() throws Exception {
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE).enableSsl(true).sslSkipCertValidation(true)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).create();
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

    @Test
    public void shouldAuthenticateWithThreads() throws Exception {
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "gmodern"));

        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final Callable<Long> countTraversalJob = () -> g.V().both().both().count().next();
        final List<Future<Long>> results = executor.invokeAll(Collections.nCopies(100, countTraversalJob));

        assertEquals(100, results.size());
        for (int ix = 0; ix < results.size(); ix++) {
            try {
                assertEquals(30L, results.get(ix).get(1000, TimeUnit.MILLISECONDS).longValue());
            } catch (Exception ex) {
                // failure but shouldn't have
                cluster.close();
                fail("Exception halted assertions - " + ex.getMessage());
            }
        }
        cluster.close();
    }

    public void assertAuthViaToStringWithSpecifiedSerializer(final MessageSerializer serializer) throws InterruptedException, ExecutionException {
        final Map<String,Object> config = new HashMap<>();
        config.put("serializeResultToString", true);
        serializer.configure(config, null);
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).serializer(serializer).create();
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
                .protocol(kdcServer.serverPrincipalName).addContactPoint(kdcServer.hostname).create();
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
