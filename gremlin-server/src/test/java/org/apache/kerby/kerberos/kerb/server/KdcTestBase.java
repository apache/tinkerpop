/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *  
 *    http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License. 
 *  
 */
package org.apache.kerby.kerberos.kerb.server;

import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.client.KrbClient;
import org.apache.kerby.kerberos.kerb.client.KrbPkinitClient;
import org.apache.kerby.kerberos.kerb.client.KrbTokenClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

public abstract class KdcTestBase {
    private static final Logger logger = LoggerFactory.getLogger(KdcTestBase.class);

    private static File testDir;
    protected static String hostname;
    private static final String clientPassword = "123456";
    private static final String clientPrincipalName = "drankye";
    private static final String clientPrincipal = clientPrincipalName + "@" + TestKdcServer.KDC_REALM;
    private static final String serverPrincipalName = "test-service";
    private static String serverPrincipal = null;

    private SimpleKdcServer kdcServer;

    KdcTestBase() {
        // Hostname setting must be consistent with the way gremlin-console sets gremlin-server's hostname
        // and derives gremlin-server's principal name. Also, the hostname needs to be lowercase for use
        // in principal names.
        try {
            hostname = Inet4Address.getLocalHost().getCanonicalHostName().toLowerCase();
            serverPrincipal = serverPrincipalName + "/" + hostname + "@" + TestKdcServer.KDC_REALM;
        } catch (UnknownHostException e) {
            logger.error("Hostname not found");
        }
    }

    @BeforeClass
    public static void createTestDir() throws IOException {
        String basedir = System.getProperty("basedir");
        if (basedir == null) {
            basedir = new File(".").getCanonicalPath();
        }
        File targetdir = new File(basedir, "target");
        testDir = new File(targetdir, "tmp");
        testDir.mkdirs();
    }

    @AfterClass
    public static void deleteTestDir() throws IOException {
        testDir.delete();
    }

    protected File getTestDir() {
        return testDir;
    }

    protected SimpleKdcServer getKdcServer() {
        return kdcServer;
    }

    protected KrbClient getKrbClient() {
        return kdcServer.getKrbClient();
    }

    protected KrbPkinitClient getPkinitClient() {
        return kdcServer.getPkinitClient();
    }

    protected KrbTokenClient getTokenClient() {
        return kdcServer.getTokenClient();
    }

    protected String getClientPrincipalName() {
        return clientPrincipalName;
    }

    protected String getClientPrincipal() {
        return clientPrincipal;
    }

    protected String getServerPrincipalName() {
        return serverPrincipalName;
    }

    protected String getClientPassword() {
        return clientPassword;
    }

    protected String getServerPrincipal() {
        return serverPrincipal;
    }

    protected String getHostname() {
        return hostname;
    }

    protected boolean allowUdp() { return false; } //}true;}

    protected boolean allowTcp() {
        return true;
    }
    
    @Before
    public void setUp() throws Exception {
        setUpKdcServer();

        createPrincipals();

        setUpClient();
    }

    protected void prepareKdc() throws KrbException {
        kdcServer.init();
    }

    protected void configKdcSeverAndClient() {
        kdcServer.setWorkDir(testDir);
    }

    protected void setUpKdcServer() throws Exception {
        kdcServer = new TestKdcServer(allowTcp(), allowUdp());

        configKdcSeverAndClient();

        prepareKdc();

        kdcServer.start();
    }

    protected void setUpClient() throws Exception {
    }

    protected void createPrincipals() throws KrbException {
        kdcServer.createPrincipals(serverPrincipal);
        kdcServer.createPrincipal(clientPrincipal, clientPassword);
    }

    protected void deletePrincipals() throws KrbException {
        kdcServer.getKadmin().deleteBuiltinPrincipals();
        kdcServer.deletePrincipals(serverPrincipal);
        kdcServer.deletePrincipal(clientPrincipal);
    }

    @After
    public void tearDown() throws Exception {
        deletePrincipals();
        kdcServer.stop();
    }
}