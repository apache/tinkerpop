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
package org.apache.kerby.kerberos.kerb.integration.test.sasl;

import org.apache.kerby.kerberos.kerb.integration.test.AppClient;
import org.apache.kerby.kerberos.kerb.integration.test.Transport;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class SaslAppClient extends AppClient {
    private SaslClient saslClient;

    public SaslAppClient(String[] args) throws Exception {
        super(args);

        String protocol = args[2];
        String serverFqdn = args[3];
        Map<String, String> props = new HashMap<String, String>();
        props.put(Sasl.QOP, "auth");

        this.saslClient = Sasl.createSaslClient(new String[]{"GSSAPI"}, null,
                protocol, serverFqdn, props, null);
    }

    public static void main(String[] args) throws Exception  {
        new SaslAppClient(args).run();
    }

    @Override
    protected void usage(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: SaslAppClient "
                    + "<server-host> <server-port> <service-protocol> <server-fqdn>");
            throw new RuntimeException("Usage: SaslAppClient "
                   + "<server-host> <server-port> <service-protocol> <server-fqdn>");
        }
    }

    @Override
    protected void withConnection(Transport.Connection conn) throws Exception {
        byte[] token = saslClient.hasInitialResponse() ? new byte[0] : null;
        token = saslClient.evaluateChallenge(token);
        conn.sendMessage("CONT", token);

        Transport.Message msg = conn.recvMessage();
        while (!saslClient.isComplete() && (isContinue(msg) || isOK(msg))) {
            byte[] respToken = saslClient.evaluateChallenge(msg.body);

            if (isOK(msg)) {
                if (respToken != null) {
                    throw new IOException("Attempting to send response after completion");
                }
                break;
            } else {
                conn.sendMessage("CONT", respToken);
                msg = conn.recvMessage();
            }
        }

        //System.out.println("Context Established! ");

        token = "Hello There!\0".getBytes(StandardCharsets.UTF_8);
        //System.out.println("Will send wrap token of size " + token.length);

        conn.sendToken(token);
        setTestOK(true);

        saslClient.dispose();
    }

    private boolean isOK(Transport.Message msg) {
        if (msg.header != null) {
            return new String(msg.header, StandardCharsets.UTF_8).equals("OK");
        }
        return false;
    }

    private boolean isContinue(Transport.Message msg) {
        if (msg.header != null) {
            return new String(msg.header, StandardCharsets.UTF_8).equals("CONT");
        }
        return false;
    }
}
