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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GValueManagerTest {

    @Test
    public void shouldMergeIntoAnotherManager() {
        final GValueManager sourceManager = new GValueManager();
        final GValueManager targetManager = new GValueManager();

        final GValue<String> unpinned = GValue.of("unpinned", "foo");
        final GValue<String> pinned = GValue.of("pinned", "bar");

        sourceManager.register(unpinned);
        sourceManager.register(pinned);
        sourceManager.pinVariable(pinned.getName());

        sourceManager.mergeInto(targetManager);

        final Set<String> variableNames = targetManager.getVariableNames();
        assertEquals(2, variableNames.size());
        assertThat(variableNames.contains("unpinned"), is(true));
        assertThat(variableNames.contains("pinned"), is(true));

        final Set<String> unpinnedVariableNames = targetManager.getUnpinnedVariableNames();
        assertEquals(1, unpinnedVariableNames.size());
        assertThat(unpinnedVariableNames.contains("unpinned"), is(true));
    }

    @Test
    public void shouldMergeManagersWithOverlappingVariables() {
        final GValueManager sourceManager = new GValueManager();
        final GValueManager targetManager = new GValueManager();

        final GValue<String> sharedVar = GValue.of("shared", "value");
        final GValue<String> sourceOnly = GValue.of("sourceOnly", "sourceValue");
        final GValue<String> targetOnly = GValue.of("targetOnly", "targetValue");

        sourceManager.register(Arrays.asList(sharedVar, sourceOnly));
        targetManager.register(targetOnly);
        sourceManager.pinVariable("shared");

        sourceManager.mergeInto(targetManager);

        assertEquals(3, targetManager.getVariableNames().size());
        assertTrue(targetManager.getVariableNames().containsAll(Arrays.asList("shared", "sourceOnly", "targetOnly")));
        assertTrue(targetManager.getPinnedVariableNames().contains("shared"));
        assertEquals(2, targetManager.getUnpinnedVariableNames().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWhenMergingConflictingGValue() {
        final GValueManager sourceManager = new GValueManager();
        final GValueManager targetManager = new GValueManager();

        // Create two GValues with same name and value
        final GValue<String> var1 = GValue.of("sameName", "sameValue");
        final GValue<String> var2 = GValue.of("sameName", "diffValue");

        sourceManager.register(var1);
        targetManager.register(var2);

        sourceManager.mergeInto(targetManager); // Should throw exception due to different values
    }

    @Test
    public void shouldHandleGValueEquality() {
        final GValueManager manager = new GValueManager();

        // Create two GValues with same name and value
        final GValue<String> var1 = GValue.of("sameName", "sameValue");
        final GValue<String> var2 = GValue.of("sameName", "sameValue");

        manager.register(var1);
        manager.register(var2); // Should not throw exception due to equality

        assertEquals(1, manager.getVariableNames().size());

        // Verify the GValue in the manager equals both original GValues
        final GValue<?> registeredVar = manager.getGValues().iterator().next();
        assertTrue(registeredVar.equals(var1));
        assertTrue(registeredVar.equals(var2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenRegisteringConflictingVariables() {
        final GValueManager manager = new GValueManager();
        final GValue<String> var1 = GValue.of("sameName", "sameValue");
        final GValue<String> var2 = GValue.of("sameName", "diffValue");

        manager.register(var1);
        manager.register(var2); // Should throw exception due to different values
    }

    @Test
    public void shouldUpdateUnpinnedVariableSuccessfully() {
        final GValueManager manager = new GValueManager();
        final GValue<String> var = GValue.of("var", "originalValue");

        manager.register(var);
        manager.updateVariable("var", "newValue");

        // Verify the variable was updated
        final Set<GValue<?>> gValues = manager.getGValues();
        final GValue<?> updatedVar = gValues.stream()
                .filter(gv -> "var".equals(gv.getName()))
                .findFirst()
                .orElse(null);

        assertNotNull(updatedVar);
        assertEquals("newValue", updatedVar.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenUpdatingNonExistentVariable() {
        final GValueManager manager = new GValueManager();
        manager.updateVariable("nonexistent", "newValue");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenUpdatingPinnedVariable() {
        final GValueManager manager = new GValueManager();
        final GValue<String> var = GValue.of("pinnedVar", "originalValue");

        manager.register(var);
        manager.pinVariable("pinnedVar");
        manager.updateVariable("pinnedVar", "newValue"); // Should throw
    }

    @Test
    public void shouldHandlePinningVariableMultipleTimes() {
        final GValueManager manager = new GValueManager();
        final GValue<String> var = GValue.of("testVar", "value");

        manager.register(var);

        boolean pinOnce = manager.pinVariable("testVar");
        assertTrue(pinOnce);

        boolean pinTwice = manager.pinVariable("testVar");
        assertFalse(pinTwice);
    }

    @Test
    public void shouldGetVariableNames() {
        final GValueManager manager = new GValueManager();
        final GValue<String> unpinned = GValue.of("unpinned", "foo");
        final GValue<String> pinned = GValue.of("pinned", "bar");

        manager.register(unpinned);
        manager.register(pinned);
        manager.pinVariable(pinned.getName());

        final Set<String> variableNames = manager.getVariableNames();
        assertEquals(2, variableNames.size());
        assertThat(variableNames.contains("unpinned"), is(true));
        assertThat(variableNames.contains("pinned"), is(true));

        final Set<String> unpinnedVariableNames = manager.getUnpinnedVariableNames();
        assertEquals(1, unpinnedVariableNames.size());
        assertThat(unpinnedVariableNames.contains("unpinned"), is(true));
    }

    @Test
    public void shouldGetAllGValues() {
        final GValueManager manager = new GValueManager();
        final GValue<String> unpinned = GValue.of("unpinned", "foo");
        final GValue<String> pinned = GValue.of("pinned", "bar");

        manager.register(unpinned);
        manager.register(pinned);
        manager.pinVariable(pinned.getName());

        final Set<GValue<?>> gValues = manager.getGValues();

        assertEquals(2, gValues.size());
        assertThat(gValues.contains(unpinned), is(true));
        assertThat(gValues.contains(pinned), is(true));
    }


    @Test
    public void shouldCheckIfManagerHasVariables() {
        final GValueManager manager = new GValueManager();

        assertThat(manager.hasVariables(), is(false));

        final GValue<String> gValue = GValue.of("x", "foo");

        manager.register(gValue);

        assertThat(manager.hasVariables(), is(true));
        assertThat(manager.hasUnpinnedVariables(), is(true));

        manager.pinVariable(gValue.getName());

        assertThat(manager.hasVariables(), is(true));
        assertThat(manager.hasUnpinnedVariables(), is(false));
    }

    @Test
    public void shouldCloneManager() throws CloneNotSupportedException {
        final GValueManager manager = new GValueManager();
        final GValue<String> unpinned = GValue.of("unpinned", "foo");
        final GValue<String> pinned = GValue.of("pinned", "bar");

        manager.register(unpinned);
        manager.register(pinned);
        manager.pinVariable(pinned.getName());

        final GValueManager clonedManager = manager.clone();

        assertThat(clonedManager.getGValues().size(), is(2));
        assertThat(clonedManager.getGValues().contains(unpinned), is(true));
        assertThat(clonedManager.getGValues().contains(pinned), is(true));

        assertThat(clonedManager.getUnpinnedVariableNames().size(), is(1));
        assertThat(clonedManager.getUnpinnedVariableNames().contains("unpinned"), is(true));

        // Verify independence - changes to clone don't affect original
        final GValue<String> newVar = GValue.of("newVar", "newValue");
        clonedManager.register(newVar);

        assertFalse(manager.getVariableNames().contains("newVar"));
        assertTrue(clonedManager.getVariableNames().contains("newVar"));
    }

    @Test
    public void shouldReturnUnmodifiableCollections() {
        final GValueManager manager = new GValueManager();
        final GValue<String> var = GValue.of("testVar", "value");

        manager.register(var);

        final Set<String> variableNames = manager.getVariableNames();
        final Set<GValue<?>> gValues = manager.getGValues();
        final Set<String> unpinnedNames = manager.getUnpinnedVariableNames();
        final Set<String> pinnedNames = manager.getPinnedVariableNames();
        final Set<GValue<?>> pinnedGValues = manager.getPinnedGValues();

        // All returned collections should be unmodifiable
        try {
            variableNames.add("newVar");
            fail("Should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // Expected
        }

        try {
            gValues.add(GValue.of("newVar", "value"));
            fail("Should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // Expected
        }

        try {
            unpinnedNames.add("newVar");
            fail("Should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // Expected
        }

        try {
            pinnedNames.add("newVar");
            fail("Should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // Expected
        }

        try {
            pinnedGValues.add(GValue.of("newVar", "value"));
            fail("Should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // Expected
        }
    }

}
