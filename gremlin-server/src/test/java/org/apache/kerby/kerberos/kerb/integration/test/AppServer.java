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

/**
 * Making it runnable because the server will be launched in a separate thread
 * in a test.
 */
public abstract class AppServer implements Runnable {
    protected Transport.Acceptor acceptor;
    private boolean terminated = false;

    public AppServer(String[] args) throws IOException {
        usage(args);

        int listenPort = Integer.parseInt(args[0]);
        this.acceptor = new Transport.Acceptor(listenPort);
    }

    protected void usage(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: AppServer <ListenPort>");
            throw new RuntimeException("Usage: AppServer <ListenPort>");
        }
    }

    public synchronized void start() {
        new Thread(this).start();
    }

    public synchronized void stop() {
        terminated = true;
    }

    @Override
    public void run() {
        try {
            synchronized (this) {
                while (!terminated) {
                    runOnce();
                }
            }
        } finally {
            acceptor.close();
        }
    }

    private void runOnce() {
        // System.out.println("Waiting for incoming connection...");

        Transport.Connection conn = acceptor.accept();
        // System.out.println("Got connection from client");

        try {
            onConnection(conn);
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

    protected abstract void onConnection(Transport.Connection conn) throws Exception;
}
