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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class SourceInstructionTest {

    private final SourceInstruction a = new SourceInstruction("atest", 1, 2, 3);
    private final SourceInstruction b = new SourceInstruction("atest", 1, 2, 3);
    private final SourceInstruction c = new SourceInstruction("atest", 1, 2);
    private final SourceInstruction d = new SourceInstruction("btest", 1, 2, 3);

    @Test
    void testMethods() {
        assertEquals("atest", a.op());
        assertEquals("btest", d.op());
        assertEquals(1, a.args()[0]);
        assertEquals(3, d.args()[2]);
        assertEquals(2, c.args()[1]);
    }

    @Test
    void testHashEquals() {
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotEquals(b, c);
        assertNotEquals(a.hashCode(), c.hashCode());
        assertNotEquals(a, d);
        assertNotEquals(c, d);
        assertNotEquals(a.hashCode(), d.hashCode());
        assertNotEquals(c.hashCode(), d.hashCode());
    }
}
