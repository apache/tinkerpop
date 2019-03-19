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
package org.apache.tinkerpop.machine.coefficient;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LongCoefficientTest {

    @Test
    public void testCoefficientMethods() {
        LongCoefficient coefficient = LongCoefficient.create();
        assertTrue(coefficient.isUnity());
        assertFalse(coefficient.isZero());
        LongCoefficient clone = coefficient.clone();
        assertTrue(clone.isUnity());
        assertNotSame(coefficient, clone);
        assertEquals(coefficient, clone);
        clone.sum(LongCoefficient.create(10L));
        assertFalse(clone.isUnity());
        assertEquals(11L, clone.value());
        assertEquals(clone.value(),clone.count());
        assertTrue(coefficient.isUnity());
        assertTrue(LongCoefficient.create(0L).isZero());
        coefficient.multiply(LongCoefficient.create(0L));
        assertTrue(coefficient.isZero());
        clone.sum(coefficient);
        assertEquals(clone.value(),clone.count());
        assertTrue(coefficient.isZero());
        assertEquals(11L, clone.value());
        clone.set(9L);
        assertEquals(9L, clone.value());
        clone.unity();
        assertEquals(1L, clone.value());
        assertTrue(clone.isUnity());
        assertNotEquals(clone, coefficient);
        clone.zero();
        assertEquals(clone.value(),clone.count());
        assertEquals(clone, coefficient);
        assertEquals(0L, clone.value());
        clone.sum(coefficient);
        assertTrue(clone.isZero());

    }
}
