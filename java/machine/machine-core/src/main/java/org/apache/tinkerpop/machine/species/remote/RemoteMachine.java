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
import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RemoteMachine implements Machine {

    private final String localTraverserServerLocation;
    private final int localTraverserServerPort;
    private final Socket machineServer;
    private final ObjectInputStream inputStream;
    private final ObjectOutputStream outputStream;


    private RemoteMachine(final int localTraverserServerPort, final String machineLocation, final int machinePort) {
        try {
            this.localTraverserServerLocation = InetAddress.getLocalHost().getHostAddress();
            this.localTraverserServerPort = localTraverserServerPort;
            this.machineServer = new Socket(machineLocation, machinePort);
            this.outputStream = new ObjectOutputStream(machineServer.getOutputStream());
            this.inputStream = new ObjectInputStream(machineServer.getInputStream());
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public <C> Bytecode<C> register(final Bytecode<C> sourceCode) {
        try {
            this.outputStream.writeObject(Request.register(sourceCode));
            this.outputStream.flush();
            return (Bytecode<C>) this.inputStream.readObject();
        } catch (final IOException | ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public <C, E> Iterator<Traverser<C, E>> submit(final Bytecode<C> bytecode) {
        try {
            final TraverserServer<C, E> traverserServer = new TraverserServer<>(this.localTraverserServerPort);
            new Thread(traverserServer).start();
            this.outputStream.writeObject(Request.submit(bytecode, this.localTraverserServerLocation, this.localTraverserServerPort));
            this.outputStream.flush();
            return traverserServer;
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public <C> void close(final Bytecode<C> sourceCode) {
        try {
            this.outputStream.writeObject(Request.close(sourceCode));
            this.outputStream.flush();
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static Machine open(final int localTraverserServerPort, final String machineLocation, final int machinePort) {
        return new RemoteMachine(localTraverserServerPort, machineLocation, machinePort);
    }

    @Override
    public void close() throws IOException {
        this.inputStream.close();
        this.outputStream.close();
        this.machineServer.close();
    }
}
