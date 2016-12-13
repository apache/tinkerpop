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

import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.server.LoginTestBase;
import org.apache.kerby.kerberos.kerb.server.TestKdcServer;
import org.apache.tinkerpop.gremlin.server.auth.Krb5Authenticator;

import java.io.File;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.LoggerFactory;


/**
 * @author Marc de Lignie
 */
public class AuthKrb5TestBase extends LoginTestBase {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(AuthKrb5TestBase.class);

    private String safeHostname = null;
    private String safeServerPrincipal = null;

    public AuthKrb5TestBase() {
        // Hostname setting must be consistent with the way gremlin-console sets gremlin-server's hostname
        // and derives gremlin-server's principal name. Also, the hostname needs to be lowercase for use
        // in principal names.
        try {
            safeHostname = Inet4Address.getLocalHost().getCanonicalHostName().toLowerCase();
            safeServerPrincipal = getServerPrincipalName() + "/" + getHostname() + "@" + TestKdcServer.KDC_REALM;
        } catch (UnknownHostException e) {
            logger.error("Hostname not found");
        }
    }

    @Rule
    public TestName name = new TestName();

    @Override
    protected String getHostname() {
        return safeHostname;
    }

    @Override
    protected String getServerPrincipal() { return safeServerPrincipal; }

    @Override
    protected void createPrincipals() throws KrbException {
        getKdcServer().createPrincipals(getServerPrincipal());
        getKdcServer().createPrincipal(getClientPrincipal(), getClientPassword());
    }

    @Override
    protected void deletePrincipals() throws KrbException {
        getKdcServer().getKadmin().deleteBuiltinPrincipals();
        getKdcServer().deletePrincipals(getServerPrincipal());
        getKdcServer().deletePrincipal(getClientPrincipal());
    }

    protected class GremlinServerAuthKrb5Integrate extends AbstractGremlinServerIntegrationTest {

        final String principal;
        final File keytabFile;

        GremlinServerAuthKrb5Integrate(String principal, File keytabFile) {
            this.principal = principal;
            this.keytabFile = keytabFile;
        }

        @Override
        public Settings overrideSettings(final Settings settings) {
            settings.host = getHostname();
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
        logger.info("Starting " + name.getMethodName());
        super.setUp();
        loginServiceUsingKeytab();
        loginClientUsingTicketCache();
        final String buildDir = System.getProperty("build.dir");
        System.setProperty("java.security.auth.login.config", buildDir +
            "/test-classes/org/apache/tinkerpop/gremlin/server/gremlin-console-jaas.conf");
    }
}
