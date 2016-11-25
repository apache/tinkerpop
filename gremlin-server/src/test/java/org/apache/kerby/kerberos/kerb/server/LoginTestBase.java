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

import org.apache.kerby.kerberos.kerb.client.JaasKrbUtil;
import org.apache.kerby.kerberos.kerb.type.ticket.TgtTicket;
import org.junit.After;
import org.junit.Before;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.security.Principal;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;


public class LoginTestBase extends KdcTestBase {

    protected File ticketCacheFile;
    protected File serviceKeytabFile;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        ticketCacheFile = new File(getTestDir(), "test-tkt.cc");
        serviceKeytabFile = new File(getTestDir(), "test-service.keytab");
    }

    protected Subject loginClientUsingPassword() throws LoginException {
        return JaasKrbUtil.loginUsingPassword(getClientPrincipal(),
            getClientPassword());
    }

    protected Subject loginClientUsingTicketCache() throws Exception {
        TgtTicket tgt = getKrbClient().requestTgt(getClientPrincipal(),
            getClientPassword());
        getKrbClient().storeTicket(tgt, ticketCacheFile);

        return JaasKrbUtil.loginUsingTicketCache(getClientPrincipal(),
            ticketCacheFile);
    }

    protected Subject loginServiceUsingKeytab() throws Exception {
        getKdcServer().exportPrincipal(getServerPrincipal(), serviceKeytabFile);
        return JaasKrbUtil.loginUsingKeytab(getServerPrincipal(),
            serviceKeytabFile);
    }

    protected void checkSubject(Subject subject) {
        Set<Principal> clientPrincipals = subject.getPrincipals();
        assertThat(clientPrincipals);
    }

    @After
    @Override
    public void tearDown() throws Exception {
//        ticketCacheFile.delete();        easier for debugging
//        serviceKeytabFile.delete();      easier for debugging

        super.tearDown();
    }
}