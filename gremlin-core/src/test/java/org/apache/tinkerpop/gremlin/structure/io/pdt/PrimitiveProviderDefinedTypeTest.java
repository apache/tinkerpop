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

public class PrimitiveProviderDefinedTypeTest {

    @Test
    public void shouldConstructWithNameAndValue() {
        final PrimitiveProviderDefinedType pdt = new PrimitiveProviderDefinedType("Uint32", "42");
        assertEquals("Uint32", pdt.getName());
        assertEquals("42", pdt.getValue());
    }

    @Test
    public void shouldAllowEmptyStringValue() {
        final PrimitiveProviderDefinedType pdt = new PrimitiveProviderDefinedType("Empty", "");
        assertEquals("", pdt.getValue());
    }

    @Test
    public void shouldThrowOnNullName() {
        assertThrows(IllegalArgumentException.class, () -> new PrimitiveProviderDefinedType(null, "v"));
    }

    @Test
    public void shouldThrowOnEmptyName() {
        assertThrows(IllegalArgumentException.class, () -> new PrimitiveProviderDefinedType("", "v"));
    }

    @Test
    public void shouldThrowOnNullValue() {
        assertThrows(IllegalArgumentException.class, () -> new PrimitiveProviderDefinedType("Name", null));
    }

    @Test
    public void shouldHaveCorrectEquals() {
        final PrimitiveProviderDefinedType a = new PrimitiveProviderDefinedType("Uint32", "007");
        final PrimitiveProviderDefinedType b = new PrimitiveProviderDefinedType("Uint32", "007");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void shouldNotEqualDifferentName() {
        final PrimitiveProviderDefinedType a = new PrimitiveProviderDefinedType("Uint32", "42");
        final PrimitiveProviderDefinedType b = new PrimitiveProviderDefinedType("Int64", "42");
        assertNotEquals(a, b);
    }

    @Test
    public void shouldNotEqualDifferentValue() {
        final PrimitiveProviderDefinedType a = new PrimitiveProviderDefinedType("Uint32", "42");
        final PrimitiveProviderDefinedType b = new PrimitiveProviderDefinedType("Uint32", "43");
        assertNotEquals(a, b);
    }

    @Test
    public void shouldHaveCorrectToString() {
        final PrimitiveProviderDefinedType pdt = new PrimitiveProviderDefinedType("Uint32", "42");
        assertEquals("pdt[Uint32](42)", pdt.toString());
    }

    @Test
    public void shouldSupportHydratedObject() {
        final PrimitiveProviderDefinedType pdt = new PrimitiveProviderDefinedType("Uint32", "42");
        assertNull(pdt.getHydrated());

        final Object hydrated = Long.valueOf(42L);
        final PrimitiveProviderDefinedType withHydrated = pdt.withHydrated(hydrated);
        assertEquals(hydrated, withHydrated.getHydrated());
        // original is unchanged
        assertNull(pdt.getHydrated());
        // logical equality is not affected by hydration
        assertEquals(pdt, withHydrated);
    }
}
