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
public final class MachineServer implements AutoCloseable {

    private static final int FLUSH_AMOUNT = 10;

    private final int machineServerPort;
    private ServerSocket machineServerSocket;
    private AtomicBoolean serverAlive = new AtomicBoolean(Boolean.FALSE);
    private final Machine machine = LocalMachine.open();

    public MachineServer(final int machineServerPort) {
        this.machineServerPort = machineServerPort;
        new Thread(this::run).start();
    }

    private void run() {
        try {
            this.serverAlive.set(Boolean.TRUE);
            this.machineServerSocket = new ServerSocket(this.machineServerPort);
            while (this.serverAlive.get()) {
                final Socket clientSocket = this.machineServerSocket.accept();
                new Thread(new Worker(clientSocket)).start();
            }
        } catch (final Exception e) {
            if (this.serverAlive.get())
                throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void close() {
        if (this.serverAlive.get()) {
            try {
                if (null != this.machineServerSocket)
                    this.machineServerSocket.close();
                this.machine.close();
                this.serverAlive.set(Boolean.FALSE);
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
            try (final ObjectInputStream input = new ObjectInputStream(this.clientSocket.getInputStream());
                 final ObjectOutputStream output = new ObjectOutputStream(this.clientSocket.getOutputStream())) {
                while (true) {
                    final RemoteMachine.Request<Object> request = (RemoteMachine.Request<Object>) input.readObject();
                    if (RemoteMachine.Request.Type.register == request.type) {
                        output.writeObject(MachineServer.this.machine.register(request.bytecode));
                        output.flush();
                    } else if (RemoteMachine.Request.Type.submit == request.type) {
                        try (final Socket traverserServerSocket = new Socket(request.traverserServerLocation, request.traverserServerPort);
                             final ObjectOutputStream traverserOutput = new ObjectOutputStream(traverserServerSocket.getOutputStream())) {
                            final Iterator<Traverser<Object, Object>> iterator = MachineServer.this.machine.submit(request.bytecode);
                            int flushCounter = 0;
                            while (iterator.hasNext()) {
                                flushCounter++;
                                traverserOutput.writeObject(iterator.next());
                                if (0 == flushCounter % FLUSH_AMOUNT) traverserOutput.flush();
                            }
                            traverserOutput.writeObject(EmptyTraverser.instance()); // this tells a TraverserServer that there are no more traversers
                            traverserOutput.flush();
                        }
                    } else { // Request.Type.unregister == request.type
                        MachineServer.this.machine.unregister(request.bytecode);
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