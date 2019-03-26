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
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RemoteMachine implements Machine, AutoCloseable {

    private final String traverserServerLocation;
    private final int traverserServerPort;
    private final Socket machineServer;
    private final ObjectInputStream inputStream;
    private final ObjectOutputStream outputStream;


    private RemoteMachine(final int traverserServerPort, final String machineServerLocation, final int machineServerPort) {
        try {
            this.traverserServerLocation = InetAddress.getLocalHost().getHostAddress();
            this.traverserServerPort = traverserServerPort;
            this.machineServer = new Socket(machineServerLocation, machineServerPort);
            this.outputStream = new ObjectOutputStream(this.machineServer.getOutputStream());
            this.inputStream = new ObjectInputStream(this.machineServer.getInputStream());
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
            final TraverserServer<C, E> traverserServer = new TraverserServer<>(this.traverserServerPort);
            this.outputStream.writeObject(Request.submit(bytecode, this.traverserServerLocation, this.traverserServerPort));
            this.outputStream.flush();
            return traverserServer;
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public <C> void unregister(final Bytecode<C> sourceCode) {
        try {
            this.outputStream.writeObject(Request.unregister(sourceCode));
            this.outputStream.flush();
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static Machine open(final int traverserServerPort, final String machineServerLocation, final int machineServerPort) {
        return new RemoteMachine(traverserServerPort, machineServerLocation, machineServerPort);
    }

    @Override
    public void close() {
        try {
            this.inputStream.close();
            this.outputStream.close();
            this.machineServer.close();
        } catch (final IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * @author Marko A. Rodriguez (http://markorodriguez.com)
     */
    static final class Request<C> implements Serializable {

        public enum Type {
            register, submit, unregister;
        }

        public final Type type;
        public final Bytecode<C> bytecode;
        final String traverserServerLocation;
        final int traverserServerPort;

        private Request(final Type type, final Bytecode<C> bytecode, final String traverserServerLocation, final int traverserServerPort) {
            this.type = type;
            this.bytecode = bytecode;
            this.traverserServerLocation = traverserServerLocation;
            this.traverserServerPort = traverserServerPort;
        }

        static <C> Request<C> register(final Bytecode<C> bytecode) {
            return new Request<>(Request.Type.register, bytecode, null, -1);
        }

        static <C> Request<C> submit(final Bytecode<C> bytecode, final String traverserServerLocation, final int traverserServerPort) {
            return new Request<>(Request.Type.submit, bytecode, traverserServerLocation, traverserServerPort);
        }

        static <C> Request<C> unregister(final Bytecode<C> bytecode) {
            return new Request<>(Request.Type.unregister, bytecode, null, -1);
        }
    }
}
