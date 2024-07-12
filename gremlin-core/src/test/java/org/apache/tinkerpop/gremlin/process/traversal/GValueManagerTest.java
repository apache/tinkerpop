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
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class GValueManagerTest {

    @Test
    public void shouldMergeIntoAnotherManager() {
        final GValueManager sourceManager = new GValueManager();
        final GValueManager targetManager = new GValueManager();

        final GValue<String> unpinned = GValue.of("unpinned", "foo");
        final GValue<String> pinned = GValue.of("pinned", "bar");

        sourceManager.track(unpinned);
        sourceManager.track(pinned);
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

//TODO::    @Test
//    public void shouldCopyRegistryState() {
//        final GValueManager manager = new GValueManager();
//        final Step sourceStep = mock(Step.class);
//        final Step targetStep = mock(Step.class);
//        final StepContract contract = mock(StepContract.class);
//
//        manager.register(sourceStep, contract);
//
//        manager.copyRegistryState(sourceStep, targetStep);
//
//        assertThat(manager.isParameterized(targetStep), is(true));
//        assertEquals(contract, manager.getStepContract(targetStep));
//    }

    @Test
    public void shouldGetVariableNames() {
        final GValueManager manager = new GValueManager();
        final GValue<String> unpinned = GValue.of("unpinned", "foo");
        final GValue<String> pinned = GValue.of("pinned", "bar");

        manager.track(unpinned);
        manager.track(pinned);
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

        manager.track(unpinned);
        manager.track(pinned);
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

        manager.track(gValue);

        assertThat(manager.hasVariables(), is(true));
        assertThat(manager.hasUnpinnedVariables(), is(true));

        manager.pinVariable(gValue.getName());

        assertThat(manager.hasVariables(), is(true));
        assertThat(manager.hasUnpinnedVariables(), is(false));
    }

    @Test
    public void shouldResetManager() {
        final GValueManager manager = new GValueManager();
        final GValue<String> unpinned = GValue.of("unpinned", "foo");
        final GValue<String> pinned = GValue.of("pinned", "bar");

        manager.track(unpinned);
        manager.track(pinned);
        manager.pinVariable(pinned.getName());

        manager.reset();

        assertThat(manager.getGValues().size(), is(0));
    }

    @Test
    public void shouldCloneManager() {
        final GValueManager manager = new GValueManager();
        final GValue<String> unpinned = GValue.of("unpinned", "foo");
        final GValue<String> pinned = GValue.of("pinned", "bar");

        manager.track(unpinned);
        manager.track(pinned);
        manager.pinVariable(pinned.getName());

        final GValueManager clonedManager = manager.clone();

        assertThat(clonedManager.getGValues().size(), is(2));
        assertThat(clonedManager.getGValues().contains(unpinned), is(true));
        assertThat(clonedManager.getGValues().contains(pinned), is(true));

        assertThat(clonedManager.getUnpinnedVariableNames().size(), is(1));
        assertThat(clonedManager.getUnpinnedVariableNames().contains("unpinned"), is(true));
    }

//    TODO::
//    @Test
//    public void shouldMergeHasContainerHolders() {
//        final GValueManager manager = new GValueManager();
//        final HasContainerHolderStep<Object, Object> sourceStep = mock(HasContainerHolderStep.class);
//        final HasContainerHolderStep<Object, Object> targetStep = mock(HasContainerHolderStep.class);
//
//        final HasContainer hasContainer1 = mock(HasContainer.class);
//        final HasContainer hasContainer2 = mock(HasContainer.class);
//
//        final List<HasContainer> sourceHasContainers = new ArrayList<>();
//        sourceHasContainers.add(hasContainer1);
//        sourceHasContainers.add(hasContainer2);
//
//        when(sourceStep.getHasContainers()).thenReturn(sourceHasContainers);
//        manager.register(sourceStep, sourceStep);
//        manager.register(targetStep, targetStep);
//
//        manager.copyRegistryState(sourceStep, targetStep);
//
//        verify(targetStep).addHasContainer(hasContainer1);
//        verify(targetStep).addHasContainer(hasContainer2);
//    }

}
