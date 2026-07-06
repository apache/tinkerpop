/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io.pdt;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class PrimitivePDTTest {

    @Test
    public void shouldConstructWithNameAndValue() {
        final PrimitivePDT pdt = new PrimitivePDT("Uint32", "42");
        assertEquals("Uint32", pdt.getName());
        assertEquals("42", pdt.getValue());
    }

    @Test
    public void shouldAllowEmptyStringValue() {
        final PrimitivePDT pdt = new PrimitivePDT("Empty", "");
        assertEquals("", pdt.getValue());
    }

    @Test
    public void shouldThrowOnNullName() {
        assertThrows(IllegalArgumentException.class, () -> new PrimitivePDT(null, "v"));
    }

    @Test
    public void shouldThrowOnEmptyName() {
        assertThrows(IllegalArgumentException.class, () -> new PrimitivePDT("", "v"));
    }

    @Test
    public void shouldThrowOnNullValue() {
        assertThrows(IllegalArgumentException.class, () -> new PrimitivePDT("Name", null));
    }

    @Test
    public void shouldHaveCorrectEquals() {
        final PrimitivePDT a = new PrimitivePDT("Uint32", "007");
        final PrimitivePDT b = new PrimitivePDT("Uint32", "007");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void shouldNotEqualDifferentName() {
        final PrimitivePDT a = new PrimitivePDT("Uint32", "42");
        final PrimitivePDT b = new PrimitivePDT("Int64", "42");
        assertNotEquals(a, b);
    }

    @Test
    public void shouldNotEqualDifferentValue() {
        final PrimitivePDT a = new PrimitivePDT("Uint32", "42");
        final PrimitivePDT b = new PrimitivePDT("Uint32", "43");
        assertNotEquals(a, b);
    }

    @Test
    public void shouldHaveCorrectToString() {
        final PrimitivePDT pdt = new PrimitivePDT("Uint32", "42");
        assertEquals("pdt[Uint32](42)", pdt.toString());
    }

    @Test
    public void shouldSupportHydratedObject() {
        final PrimitivePDT pdt = new PrimitivePDT("Uint32", "42");
        assertNull(pdt.getHydrated());

        final Object hydrated = Long.valueOf(42L);
        final PrimitivePDT withHydrated = pdt.withHydrated(hydrated);
        assertEquals(hydrated, withHydrated.getHydrated());
        // original is unchanged
        assertNull(pdt.getHydrated());
        // logical equality is not affected by hydration
        assertEquals(pdt, withHydrated);
    }
}
