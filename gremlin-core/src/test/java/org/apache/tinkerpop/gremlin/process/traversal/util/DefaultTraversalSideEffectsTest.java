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

package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversalSideEffectsTest {

    @Test
    public void shouldAllowNulls() {
        final TraversalSideEffects sideEffects = new DefaultTraversalSideEffects();
        sideEffects.register("k", new ConstantSupplier<>(null), null);
        assertNull(sideEffects.get("k"));

        sideEffects.set("k", "x");
        assertEquals("x", sideEffects.get("k"));

        sideEffects.set("k", null);
        assertNull(sideEffects.get("k"));

        sideEffects.add("k", null);
        assertNull(sideEffects.get("k"));

        sideEffects.register("kand", new ConstantSupplier<>(null), Operator.and);
        sideEffects.add("kand", null);
        assertNull(sideEffects.get("kand"));

        sideEffects.add("kand", true);
        assertThat(sideEffects.get("kand"), is(true));
    }

    @Test
    public void shouldOperateCorrectly() {
        final TraversalSideEffects sideEffects = new DefaultTraversalSideEffects();
        try {
            sideEffects.set("a", "marko");
            fail("The key was not registered: a");
        } catch (final IllegalArgumentException e) {
            assertEquals(TraversalSideEffects.Exceptions.sideEffectKeyDoesNotExist("a").getMessage(), e.getMessage());
        }
        for (final String key : Arrays.asList("a", "b", "c")) {
            try {
                sideEffects.get(key);
                fail("The key was not registered: " + key);
            } catch (final IllegalArgumentException e) {
                assertEquals(TraversalSideEffects.Exceptions.sideEffectKeyDoesNotExist(key).getMessage(), e.getMessage());
            }
        }

        sideEffects.register("a", new ConstantSupplier<>("temp"), null);
        sideEffects.register("b", new ConstantSupplier<>(0), Operator.sum);
        sideEffects.register("c", null, Operator.and);

        assertEquals(0, sideEffects.<Integer>get("b").intValue());
        try {
            sideEffects.get("c");
            fail("The key was not registered with a supplier: c");
        } catch (final IllegalArgumentException e) {
            assertEquals(TraversalSideEffects.Exceptions.sideEffectKeyDoesNotExist("c").getMessage(), e.getMessage());
        }

        sideEffects.set("a", "marko");
        sideEffects.set("b", 1);
        sideEffects.set("c", true);
        assertFalse(sideEffects.exists("blah"));
        assertTrue(sideEffects.exists("a"));
        assertEquals("marko", sideEffects.get("a"));
        assertTrue(sideEffects.exists("b"));
        assertEquals(1, sideEffects.<Integer>get("b").intValue());
        assertTrue(sideEffects.exists("c"));
        assertEquals(true, sideEffects.get("c"));

        sideEffects.add("a", "rodriguez");
        sideEffects.add("b", 2);
        sideEffects.add("c", false);
        assertTrue(sideEffects.exists("a"));
        assertEquals("rodriguez", sideEffects.get("a"));
        assertTrue(sideEffects.exists("b"));
        assertEquals(3, sideEffects.<Integer>get("b").intValue());
        assertTrue(sideEffects.exists("c"));
        assertEquals(false, sideEffects.get("c"));

        sideEffects.set("c", true);
        assertEquals(true, sideEffects.get("c"));
    }

    @Test
    public void shouldMergeCorrectly() {
        final TraversalSideEffects sideEffects = new DefaultTraversalSideEffects();
        final TraversalSideEffects other =new DefaultTraversalSideEffects();

        sideEffects.register("a", new ConstantSupplier<>("temp"), null);
        sideEffects.register("b", new ConstantSupplier<>(0), Operator.sum);
        sideEffects.register("c", null, Operator.and);

        sideEffects.mergeInto(other);

        assertEquals("temp",other.get("a"));
        assertEquals(0,other.<Integer>get("b").intValue());
        try {
            other.get("c");
            fail("The key was not registered with a supplier: c");
        } catch (final IllegalArgumentException e) {
            assertEquals(TraversalSideEffects.Exceptions.sideEffectKeyDoesNotExist("c").getMessage(), e.getMessage());
        }

        other.set("a", "marko");
        other.set("b", 1);
        other.set("c", true);
        assertFalse(other.exists("blah"));
        assertTrue(other.exists("a"));
        assertEquals("marko", other.get("a"));
        assertTrue(other.exists("b"));
        assertEquals(1, other.<Integer>get("b").intValue());
        assertTrue(other.exists("c"));
        assertEquals(true, other.get("c"));

        assertEquals("temp",sideEffects.get("a"));
        assertEquals(0,sideEffects.<Integer>get("b").intValue());
        try {
            sideEffects.get("c");
            fail("The key was not registered with a supplier: c");
        } catch (final IllegalArgumentException e) {
            assertEquals(TraversalSideEffects.Exceptions.sideEffectKeyDoesNotExist("c").getMessage(), e.getMessage());
        }
    }
}
