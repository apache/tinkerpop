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
package org.apache.tinkerpop.machine.species.io;

import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserSet;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraverserServer<C, S> implements Runnable, Iterator<Traverser<C, S>> {

    private final TraverserSet<C, S> traverserSet = new TraverserSet<>();
    private final int serverPort;
    private ServerSocket serverSocket;
    private AtomicBoolean serverAlive = new AtomicBoolean(Boolean.FALSE);

    public TraverserServer(final int serverPort) {
        this.serverPort = serverPort;
    }

    public void run() {
        try {
            this.serverSocket = new ServerSocket(this.serverPort);
            this.serverAlive.set(Boolean.TRUE);
            // System.out.println("Server started: " + this.serverSocket.toString());
            while (this.isAlive()) {
                final Socket clientSocket = this.serverSocket.accept();
                new Thread(new Worker(clientSocket)).start();
            }
            // System.out.println("Server Stopped.");
        } catch (final Exception e) {
            if (this.serverAlive.get())
                throw new RuntimeException(e.getMessage(), e);
        }
    }


    private boolean isAlive() {
        return this.serverAlive.get();
    }

    public synchronized void stop() {
        try {
            this.serverAlive.set(Boolean.FALSE);
            this.serverSocket.close();
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasNext() {
        if (!this.traverserSet.isEmpty())
            return true;
        else {
            while (this.isAlive()) {
                if (!this.traverserSet.isEmpty())
                    return true;
            }
            return !this.traverserSet.isEmpty();
        }
    }

    @Override
    public Traverser<C, S> next() {
        return this.traverserSet.remove();
    }

    public class Worker implements Runnable {

        private final Socket clientSocket;

        Worker(final Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        public void run() {
            //int counter = 0;
            try {
                //System.out.println("Client connected: " + this.clientSocket.toString());
                final ObjectInputStream input = new ObjectInputStream(this.clientSocket.getInputStream());
                while (true) {
                    final Traverser<C, S> traverser = (Traverser<C, S>) input.readObject();
                    //System.out.println("Received traverser [" + this.clientSocket.getPort() + "]: " + traverser);
                    traverserSet.add(traverser);
                }
                //System.out.println(this.toString() + ": is complete..." + counter);
            } catch (final EOFException e) {
                // okay -- this is how the worker closes
            } catch (final IOException | ClassNotFoundException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }


}
