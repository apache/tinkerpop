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
package org.apache.tinkerpop.machine;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.compiler.CommonCompiler;
import org.apache.tinkerpop.machine.strategy.finalization.CoefficientStrategy;
import org.apache.tinkerpop.machine.strategy.optimization.IdentityStrategy;
import org.apache.tinkerpop.machine.strategy.verification.CoefficientVerificationStrategy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class LocalMachineTest {

    @Test
    void testLocalMachineStateChanges() {
        final LocalMachine machine = (LocalMachine) LocalMachine.open();
        assertEquals(0, machine.sources.size());
        ///
        final Bytecode<Long> sourceCode = new Bytecode<>();
        sourceCode.addSourceInstruction(CommonCompiler.Symbols.WITH_STRATEGY, IdentityStrategy.class);
        sourceCode.addSourceInstruction(CommonCompiler.Symbols.WITH_STRATEGY, CoefficientStrategy.class);
        assertEquals(2, sourceCode.getSourceInstructions().size());
        assertEquals(0, sourceCode.getInstructions().size());
        ///
        final Bytecode<Long> bytecode = machine.register(sourceCode);
        assertEquals(1, machine.sources.size());
        assertNotEquals(sourceCode, bytecode);
        assertEquals(1, bytecode.getSourceInstructions().size());
        assertEquals(0, bytecode.getInstructions().size());
        assertEquals(LocalMachine.WITH_SOURCE_CODE, bytecode.getSourceInstructions().get(0).op());
        assertEquals(1, bytecode.getSourceInstructions().get(0).args().length);
        ///
        assertFalse(machine.submit(bytecode).hasNext());
        assertEquals(1, machine.sources.size());
        assertEquals(1, bytecode.getSourceInstructions().size());
        assertEquals(0, bytecode.getInstructions().size());
        assertEquals(LocalMachine.WITH_SOURCE_CODE, bytecode.getSourceInstructions().get(0).op());
        assertEquals(1, bytecode.getSourceInstructions().get(0).args().length);
        assertFalse(machine.submit(bytecode).hasNext());
        assertEquals(1, machine.sources.size());
        assertEquals(1, bytecode.getSourceInstructions().size());
        assertEquals(0, bytecode.getInstructions().size());
        assertEquals(LocalMachine.WITH_SOURCE_CODE, bytecode.getSourceInstructions().get(0).op());
        assertEquals(1, bytecode.getSourceInstructions().get(0).args().length);
        ///
        assertEquals(bytecode, machine.register(bytecode)); // if the same source is already registered, don't re-register
        assertEquals(bytecode, machine.register(bytecode));
        assertEquals(1, machine.sources.size());
        ///
        bytecode.addSourceInstruction(CommonCompiler.Symbols.WITH_STRATEGY, CoefficientVerificationStrategy.class);
        assertFalse(machine.submit(bytecode).hasNext());
        assertEquals(1, machine.sources.size());
        assertNotEquals(bytecode, machine.register(bytecode));
        assertEquals(2, machine.sources.size());
        machine.close(bytecode);
        assertEquals(1, machine.sources.size());
    }
}
