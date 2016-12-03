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

import org.apache.kerby.kerberos.kerb.server.LoginTestBase;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.server.auth.Krb5Authenticator;
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

    @Rule
    public TestName name = new TestName();

    private class GremlinServerAuthKrb5Integrate extends AbstractGremlinServerIntegrationTest {

        private final String nameOfTest;

        GremlinServerAuthKrb5Integrate(String nameOfTest) {
            this.nameOfTest = nameOfTest;
        }

        @Override
        public Settings overrideSettings(final Settings settings) {
            logger.debug("Testname: " + nameOfTest);
            settings.host = hostname;
            logger.debug("Hostname: " + settings.host);
            final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
            authSettings.className = Krb5Authenticator.class.getName();
            final Map authConfig = new HashMap<String,Object>();
            final String buildDir = System.getProperty("build.dir");
            authConfig.put("keytab", serviceKeytabFile.getAbsolutePath());
            authConfig.put("principal", getServerPrincipal());
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
        // Create keytab for gremlinServer and ticketcache for gremlinConsole
        Subject gremlinServer = loginServiceUsingKeytab();
        Subject gremlinConsole = loginClientUsingTicketCache();
        logger.debug("Done creating keytab and ticketcache");

        final String buildDir = System.getProperty("build.dir");
        System.setProperty("java.security.auth.login.config", buildDir +
            "/test-classes/org/apache/tinkerpop/gremlin/server/gremlin-console-jaas.conf");
        logger.debug("java.security.auth.login.config: " + System.getProperty("java.security.auth.login.config"));
        logger.debug("project.build.directory: " + System.getProperty("project.build.directory"));
        server = new GremlinServerAuthKrb5Integrate(name.getMethodName());
        server.setUp();
    }

    @After
    public void tearDown() throws Exception {
        server.tearDown();
        super.tearDown();
    }

    @Test
    public void shouldAuthenticate() throws Exception {
        File f = new File(".");
        logger.debug("Working dir: " + f.getCanonicalPath());
        final Cluster cluster = TestClientFactory.build().jaasEntry(TESTCONSOLE)
            .protocol("test-service").addContactPoint(hostname).enableSsl(false).create();
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
    public void shouldAuthenticateWithSerializeResultToString() throws Exception {
//        File f = new File(".");
//        logger.debug("Working dir: " + f.getCanonicalPath());
//        MessageSerializer serializer = new GryoMessageSerializerV1d0();
//        Map config = new HashMap<String, Object>();
//        config.put("serializeResultToString", true);
//        serializer.configure(config, null);
//        final Cluster cluster = Cluster.build().jaasEntry(TESTCONSOLE)
//                .protocol("test-service").addContactPoint(serverHostname).port(45940).enableSsl(false).serializer(serializer).create();
//        final Client client = cluster.connect();
//        try {
//            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
//            assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
//            assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
//        } finally {
//            cluster.close();
//        }
    }

    // ToDo: test client login fails as wanted without a valid ticket
    // ToDo: test with System.setProperty("javax.security.sasl.qop", "auth-conf");
}
