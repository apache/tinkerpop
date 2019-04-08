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
package org.apache.tinkerpop.machine.species.remote;

import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserSet;
import org.apache.tinkerpop.machine.traverser.species.EmptyTraverser;

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
public final class TraverserServer<C, S> implements AutoCloseable, Iterator<Traverser<C, S>> {

    private final TraverserSet<C, S> traverserSet = new TraverserSet<>();
    private final int serverPort;
    private ServerSocket serverSocket;
    private AtomicBoolean serverAlive = new AtomicBoolean(Boolean.TRUE);

    public TraverserServer(final int serverPort) {
        this.serverPort = serverPort;
        new Thread(this::run).start();
    }

    private void run() {
        try {
            this.serverSocket = new ServerSocket(this.serverPort);
            while (this.serverAlive.get()) {
                final Socket clientSocket = this.serverSocket.accept();
                new Thread(new Worker(clientSocket)).start();
            }
        } catch (final Exception e) {
            if (this.serverAlive.get())
                throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasNext() {
        if (!this.traverserSet.isEmpty())
            return true;
        else {
            while (this.serverAlive.get()) {
                if (!this.traverserSet.isEmpty())
                    return true;
            }
            return !this.traverserSet.isEmpty();
        }
    }

    @Override
    public Traverser<C, S> next() {
        if (!this.traverserSet.isEmpty())
            return this.traverserSet.remove();
        else {
            while (this.serverAlive.get()) {
                if (!this.traverserSet.isEmpty())
                    return this.traverserSet.remove();
            }
            return this.traverserSet.remove();
        }
    }

    @Override
    public synchronized void close() {
        if (this.serverAlive.get()) {
            try {
                this.serverAlive.set(Boolean.FALSE);
                this.serverSocket.close();
            } catch (final IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    private class Worker implements Runnable {

        private final Socket clientSocket;

        private Worker(final Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        public void run() {
            try (final ObjectInputStream input = new ObjectInputStream(this.clientSocket.getInputStream())) {
                while (true) {
                    final Traverser<C, S> traverser = (Traverser<C, S>) input.readObject();
                    if (traverser instanceof EmptyTraverser) { // EmptyTraverser kills server
                        TraverserServer.this.close();
                        break;
                    } else
                        TraverserServer.this.traverserSet.add(traverser);
                }
            } catch (final EOFException e) {
                // okay -- this is how the worker closes
            } catch (final IOException | ClassNotFoundException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }
}
