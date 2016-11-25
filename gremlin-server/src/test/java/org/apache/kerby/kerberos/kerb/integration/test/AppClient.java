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
package org.apache.kerby.kerberos.kerb.integration.test;

import java.io.IOException;

public abstract class AppClient {
    private Transport.Connection conn;
    private boolean isTestOK = false;

    public AppClient(String[] args) throws Exception {
        usage(args);

        String hostName = args[0];
        int port = Integer.parseInt(args[1]);

        this.conn = Transport.Connector.connect(hostName, port);
    }

    protected void usage(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java <options> AppClient "
                    + "<server-host> <server-port>");
           throw new RuntimeException("Arguments are invalid.");
        }
    }

    public void run() {
        // System.out.println("Connected to server");

        try {
            withConnection(conn);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected abstract void withConnection(Transport.Connection conn) throws Exception;

    public boolean isTestOK() {
        return isTestOK;
    }

    protected synchronized void setTestOK(boolean isOK) {
        this.isTestOK = isOK;
    }
}
