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
import org.apache.kerby.kerberos.kerb.client.KrbConfig;
import org.apache.kerby.kerberos.kerb.client.KrbConfigKey;
import org.apache.kerby.util.NetworkUtil;

/*
 * Except for this comment, this file is a literal copy from:
 *     https://github.com/apache/directory-kerby/blob/kerby-all-1.0.0-RC2/
 *         kerby-kerb/kerb-kdc-test/src/test/java/org/apache/kerby/kerberos/kerb/server/TestKdcServer.java
 *
 * See also: gremlin-server/src/main/static/NOTICE
 */
public class TestKdcServer extends SimpleKdcServer {
    public static final String KDC_REALM = "TEST.COM";
    public static final String HOSTNAME = "localhost";

    public TestKdcServer(boolean allowTcp, boolean allowUdp) throws KrbException {
        super();

        setKdcRealm(KDC_REALM);
        setKdcHost(HOSTNAME);
        setAllowTcp(allowTcp);
        setAllowUdp(allowUdp);

        if (allowTcp) {
            setKdcTcpPort(NetworkUtil.getServerPort());
        }
        if (allowUdp) {
            setKdcUdpPort(NetworkUtil.getServerPort());
        }

        KrbClient krbClnt = getKrbClient();
        KrbConfig krbConfig = krbClnt.getKrbConfig();
        krbConfig.setString(KrbConfigKey.PERMITTED_ENCTYPES,
                "aes128-cts-hmac-sha1-96 des-cbc-crc des-cbc-md5 des3-cbc-sha1");

        krbClnt.setTimeout(10 * 1000);
    }
}