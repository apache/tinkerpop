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
package org.apache.tinkerpop.machine.bytecode;

import org.apache.tinkerpop.machine.coefficient.LongCoefficient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class BytecodeTest {

    @Test
    void shouldHaveParents() {
        Bytecode<Long> parent = new Bytecode<>();
        assertFalse(parent.getParent().isPresent());
        Bytecode<Long> child = new Bytecode<>();
        parent.addInstruction(LongCoefficient.create(), "test", child);
        assertTrue(child.getParent().isPresent());
        assertEquals(parent, child.getParent().get());
        assertFalse(child.getParent().get().getParent().isPresent());
    }

    @Test
    void shouldCloneParents() {
        Bytecode<Long> parent = new Bytecode<>();
        assertFalse(parent.getParent().isPresent());
        Bytecode<Long> child = new Bytecode<>();
        parent.addInstruction(LongCoefficient.create(), "test", child);

        Bytecode<Long> cloneParent = parent.clone();
        Bytecode<Long> cloneChild = (Bytecode<Long>) cloneParent.lastInstruction().args()[0];
        assertTrue(cloneChild.getParent().isPresent());
        assertEquals(cloneParent, cloneChild.getParent().get());
        assertFalse(cloneParent.getParent().isPresent());
        cloneParent.addArgs("hello");
        assertNotEquals(cloneParent, parent);
    }
}
