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
package org.apache.tinkerpop.machine.species;

import org.apache.tinkerpop.machine.Machine;
import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BasicMachine implements Machine {

    private BasicMachine() {
        // use open();
    }

    @Override
    public <C> Bytecode<C> register(final Bytecode<C> sourceCode) {
        return sourceCode;
    }

    @Override
    public <C> void close(final Bytecode<C> sourceCode) {

    }

    @Override
    public <C, E> Iterator<Traverser<C, E>> submit(final Bytecode<C> bytecode) {
        return Compilation.<C, Object, E>compile(bytecode).getProcessor();
    }

    public static Machine open() {
        return new BasicMachine();
    }

    @Override
    public void close() throws IOException {

    }
}
