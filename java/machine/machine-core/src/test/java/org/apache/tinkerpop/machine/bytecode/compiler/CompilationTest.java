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
package org.apache.tinkerpop.machine.bytecode.compiler;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.coefficient.LongCoefficient;
import org.apache.tinkerpop.machine.function.CFunction;
import org.apache.tinkerpop.machine.structure.data.TMap;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class CompilationTest {

    @Test
    void shouldCloneCorrectly() {
        final Bytecode<Long> bytecode = new Bytecode<>();
        final Bytecode<Long> inner = new Bytecode<>();
        inner.addInstruction(LongCoefficient.create(), CommonCompiler.Symbols.VALUE, "name");
        bytecode.addInstruction(LongCoefficient.create(), CommonCompiler.Symbols.HAS_KEY, "eq", inner);
        bytecode.addInstruction(LongCoefficient.create(), CommonCompiler.Symbols.COUNT);

        final Compilation<Long, TMap, Long> compilationA = Compilation.compile(bytecode);
        final Compilation<Long, TMap, Long> compilationB = compilationA.clone();
        assertEquals(compilationA.hashCode(), compilationB.hashCode());
        assertEquals(compilationA, compilationB);
        assertNotSame(compilationA, compilationB);

        final List<CFunction<Long>> listA = compilationA.getFunctions();
        final List<CFunction<Long>> listB = compilationB.getFunctions();
        assertEquals(listA.hashCode(), listB.hashCode());
        assertEquals(listA, listB);
        assertNotSame(listA, listB);
        for (int i = 0; i < listA.size(); i++) {
            assertEquals(listA.get(i).hashCode(), listB.get(i).hashCode());
            assertEquals(listA.get(i), listB.get(i));
            assertNotSame(listA.get(i), listB.get(i));
        }
    }
}
