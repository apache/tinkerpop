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

import org.apache.tinkerpop.machine.Machine;
import org.apache.tinkerpop.machine.species.LocalMachine;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.species.EmptyTraverser;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MachineServer implements Runnable, Closeable {

    private final int serverPort;
    private ServerSocket serverSocket;
    private AtomicBoolean serverAlive = new AtomicBoolean(Boolean.TRUE);
    private final Machine machine = LocalMachine.open();

    public MachineServer(final int serverPort) {
        this.serverPort = serverPort;
        new Thread(this).start();
    }

    public void run() {
        try {
            this.serverSocket = new ServerSocket(this.serverPort);
            while (this.isAlive()) {
                final Socket clientSocket = this.serverSocket.accept();
                new Thread(new Worker(clientSocket)).start();
            }
        } catch (final Exception e) {
            if (this.serverAlive.get())
                throw new RuntimeException(e.getMessage(), e);
        }
    }

    private boolean isAlive() {
        return this.serverAlive.get();
    }

    public synchronized void close() {
        try {
            this.serverAlive.set(Boolean.FALSE);
            this.serverSocket.close();
            this.machine.close();
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public class Worker implements Runnable {

        private final Socket clientSocket;

        Worker(final Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        public void run() {
            try {
                final ObjectInputStream input = new ObjectInputStream(this.clientSocket.getInputStream());
                final ObjectOutputStream output = new ObjectOutputStream(this.clientSocket.getOutputStream());
                while (true) {
                    final Request<Object> request = (Request) input.readObject();
                    if (Request.Type.register == request.type) {
                        output.writeObject(machine.register(request.bytecode));
                        output.flush();
                    } else if (Request.Type.submit == request.type) {
                        final Socket traverserServerSocket = new Socket(request.traverserServerLocation, request.traverserServerPort);
                        final ObjectOutputStream traverserOutput = new ObjectOutputStream(traverserServerSocket.getOutputStream());
                        final Iterator<Traverser<Object, Object>> iterator = machine.submit(request.bytecode);
                        while (iterator.hasNext()) {
                            traverserOutput.writeObject(iterator.next());
                            traverserOutput.flush();
                        }
                        traverserOutput.writeObject(EmptyTraverser.instance());
                        traverserOutput.flush();
                        traverserOutput.close();
                    } else if (Request.Type.close == request.type) {
                        machine.close(request.bytecode);
                    } else {
                        throw new IllegalStateException("This shouldn't happen: " + request);
                    }
                }
            } catch (final EOFException e) {
                // okay -- this is how the worker closes
            } catch (final IOException | ClassNotFoundException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }
}