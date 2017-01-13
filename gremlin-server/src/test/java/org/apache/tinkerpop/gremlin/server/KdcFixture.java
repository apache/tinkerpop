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
import org.apache.kerby.kerberos.kerb.client.KrbClient;
import org.apache.kerby.kerberos.kerb.client.KrbConfig;
import org.apache.kerby.kerberos.kerb.client.KrbConfigKey;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.kerby.kerberos.kerb.type.ticket.TgtTicket;
import org.apache.kerby.util.NetworkUtil;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.Inet4Address;


/**
 * @author Marc de Lignie
 *
 * This class is derived from the following classes from https://github.com/apache/directory-kerby/blob/kerby-all-1.0.0-RC2/:
 *  - org.apache.kerby.kerberos.kerb.server.TestKdcServer
 *  - org.apache.kerby.kerberos.kerb.server.KdcTestBase
 *  - org.apache.kerby.kerberos.kerb.server.LoginTestBase.java
 *
 * See also: gremlin-server/src/main/static/NOTICE
 */
public class KdcFixture {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(KdcFixture.class);

    final String clientPassword = "123456";
    final String clientPrincipalName = "drankye";
    final String serverPrincipalName = "test-service";
    final String ticketCacheFileName = "test-tkt.cc";
    final String serviceKeytabFileName = "test-service.keytab";

    final String clientPrincipal;
    final String serverPrincipal;
    final File testDir;
    final File ticketCacheFile;
    final File serviceKeytabFile;

    final String hostname;
    private SimpleKdcServer kdcServer;


    public KdcFixture(String authConfigName)  throws Exception {
        System.setProperty("java.security.auth.login.config", authConfigName);
        hostname = findHostname();
        serverPrincipal = serverPrincipalName + "/" + hostname + "@" + KdcFixture.TestKdcServer.KDC_REALM;
        clientPrincipal = clientPrincipalName + "@" + KdcFixture.TestKdcServer.KDC_REALM;
        testDir = createTestDir();
        ticketCacheFile = new File(testDir, ticketCacheFileName);
        serviceKeytabFile = new File(testDir, serviceKeytabFileName);
    }

    private String findHostname() {
        // Hostname setting must be consistent with the way gremlin-console sets gremlin-server's hostname
        // and derives gremlin-server's principal name. Also, the hostname needs to be lowercase for use
        // in principal names.
        String hostname = "";
        try {
            hostname = Inet4Address.getLocalHost().getCanonicalHostName().toLowerCase();
        } catch (Exception e) {
            logger.error("Hostname not found: " + e.getMessage());
        }
        return hostname;
    }

    private File createTestDir() {
        String basedir = System.getProperty("basedir");
        File targetdir = new File(basedir, "target");
        File testDir = new File(targetdir, "tmp");
        testDir.mkdirs();
        return testDir;
    }

    private class TestKdcServer extends SimpleKdcServer {
        public static final String KDC_REALM = "TEST.COM";
        public static final String HOSTNAME = "localhost";

        TestKdcServer() throws KrbException {
            setKdcRealm(KDC_REALM);
            setKdcHost(HOSTNAME);
            setAllowTcp(true);
            setAllowUdp(false);    // There are still udp issues in Apache Directory-Kerby 1.0.0-RC2
            setKdcTcpPort(NetworkUtil.getServerPort());

            KrbClient krbClnt = getKrbClient();
            KrbConfig krbConfig = krbClnt.getKrbConfig();
            krbConfig.setString(KrbConfigKey.PERMITTED_ENCTYPES,
                    "aes128-cts-hmac-sha1-96 des-cbc-crc des-cbc-md5 des3-cbc-sha1");
            krbClnt.setTimeout(10 * 1000);
        }
    }

    public void setUp() throws Exception {
        setUpKdcServer();
        setUpPrincipals();
    }

    private void setUpKdcServer() throws Exception {
        kdcServer = new TestKdcServer();
        kdcServer.setWorkDir(testDir);
        kdcServer.init();
        kdcServer.start();
    }

    private void setUpPrincipals() throws KrbException {
        kdcServer.createPrincipals(serverPrincipal);
        kdcServer.exportPrincipal(serverPrincipal, serviceKeytabFile);

        kdcServer.createPrincipal(clientPrincipal, clientPassword);
        TgtTicket tgt = kdcServer.getKrbClient().requestTgt(clientPrincipal, clientPassword);
        kdcServer.getKrbClient().storeTicket(tgt, ticketCacheFile);
    }

    public void close() throws Exception {
        deletePrincipals();
        kdcServer.stop();
        ticketCacheFile.delete();
        serviceKeytabFile.delete();
        testDir.delete();
        System.clearProperty("java.security.auth.login.config");
    }

    private void deletePrincipals() throws KrbException {
        kdcServer.getKadmin().deleteBuiltinPrincipals();
        kdcServer.deletePrincipals(serverPrincipal);
        kdcServer.deletePrincipal(clientPrincipal);
    }

    public void createPrincipal(final String principal) throws KrbException {
        kdcServer.createPrincipal(principal);
    }
}
