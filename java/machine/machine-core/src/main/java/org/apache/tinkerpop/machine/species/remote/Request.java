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

import org.apache.tinkerpop.machine.bytecode.Bytecode;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class Request<C> implements Serializable {

    public enum Type {
        register, submit, close;
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
        return new Request<>(Type.register, bytecode, null, -1);
    }

    static <C> Request<C> submit(final Bytecode<C> bytecode, final String traverserServerLocation, final int traverserServerPort) {
        return new Request<>(Type.submit, bytecode, traverserServerLocation, traverserServerPort);
    }

    static <C> Request<C> close(final Bytecode<C> bytecode) {
        return new Request<>(Type.close, bytecode, null, -1);
    }
}
