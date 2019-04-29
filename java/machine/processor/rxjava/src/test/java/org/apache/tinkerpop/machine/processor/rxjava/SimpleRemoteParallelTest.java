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
package org.apache.tinkerpop.machine.processor.rxjava;

import org.apache.tinkerpop.machine.AbstractTestSuite;
import org.apache.tinkerpop.machine.SimpleTestSuite;
import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.compiler.CoreCompiler.Symbols;
import org.apache.tinkerpop.machine.species.remote.MachineServer;
import org.apache.tinkerpop.machine.species.remote.RemoteMachine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class SimpleRemoteParallelTest extends SimpleTestSuite {

    private final static Bytecode<Long> BYTECODE = new Bytecode<>();
    private static MachineServer SERVER = new MachineServer(7777);

    static {
        BYTECODE.addSourceInstruction(Symbols.WITH_PROCESSOR, RxJavaProcessor.class, Map.of(RxJavaProcessor.RX_THREAD_POOL_SIZE, Runtime.getRuntime().availableProcessors() - 1));
    }

    SimpleRemoteParallelTest() {
        super(RemoteMachine.open(6666, "localhost", 7777), BYTECODE);
    }

    @AfterEach
    void delayShutdown() {
        AbstractTestSuite.sleep(100);
    }

    @AfterAll
    void stopServer() {
        SERVER.close();
        AbstractTestSuite.sleep(100);
    }

}