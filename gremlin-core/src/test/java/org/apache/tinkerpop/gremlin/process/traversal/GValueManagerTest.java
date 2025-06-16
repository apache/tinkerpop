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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddEdgeContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddPropertyContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddVertexContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.CallContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.EdgeLabelContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.ElementIdsContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.MergeElementContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.PredicateContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.RangeContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.StepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.TailContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.withSettings;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GValueManagerTest {

    @Test
    public void shouldRegisterStep() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final StepContract contract = mock(StepContract.class);

        manager.register(step, contract);

        assertThat(manager.isParameterized(step), is(true));
        assertEquals(contract, manager.getStepContract(step));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenRegisteringStepAfterLock() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final StepContract contract = mock(StepContract.class);
        manager.lock();

        manager.register(step, contract);
    }

    @Test
    public void shouldLockManager() {
        final GValueManager manager = new GValueManager();

        manager.lock();

        assertThat(manager.isLocked(), is(true));
    }

    @Test
    public void shouldNotAffectStateWhenLockingTwice() {
        final GValueManager manager = new GValueManager();
        manager.lock();

        // second lock should have no effect
        manager.lock();

        assertThat(manager.isLocked(), is(true));
    }

    @Test
    public void shouldMergeIntoAnotherManager() {
        final GValueManager sourceManager = new GValueManager();
        final GValueManager targetManager = new GValueManager();

        final Step step = mock(Step.class);
        final StepContract contract = mock(StepContract.class);
        sourceManager.register(step, contract);

        sourceManager.mergeInto(targetManager);

        assertThat(targetManager.isParameterized(step), is(true));
        assertEquals(contract, targetManager.getStepContract(step));
    }

    @Test
    public void shouldCopyRegistryState() {
        final GValueManager manager = new GValueManager();
        final Step sourceStep = mock(Step.class);
        final Step targetStep = mock(Step.class);
        final StepContract contract = mock(StepContract.class);

        manager.register(sourceStep, contract);

        manager.copyRegistryState(sourceStep, targetStep);

        assertThat(manager.isParameterized(targetStep), is(true));
        assertEquals(contract, manager.getStepContract(targetStep));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenCopyingRegistryStateAfterLock() {
        final GValueManager manager = new GValueManager();
        final Step sourceStep = mock(Step.class);
        final Step targetStep = mock(Step.class);

        final TailContract<GValue<Long>> contract = mock(TailContract.class);
        when(contract.getLimit()).thenReturn(GValue.ofLong(5L));

        manager.register(sourceStep, contract);
        manager.lock();

        manager.copyRegistryState(sourceStep, targetStep);
    }

    @Test
    public void shouldGetStepContract() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final StepContract contract = mock(StepContract.class);

        manager.register(step, contract);

        final StepContract result = manager.getStepContract(step);

        assertEquals(contract, result);
    }

    @Test
    public void shouldGetGValuesForStep() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final RangeContract<GValue<Long>> contract = mock(RangeContract.class);

        final GValue<Long> lowRange = GValue.ofLong("low", 1L);
        final GValue<Long> highRange = GValue.ofLong("high", 10L);

        when(contract.getLowRange()).thenReturn(lowRange);
        when(contract.getHighRange()).thenReturn(highRange);

        manager.register(step, contract);

        final Collection<GValue<?>> result = manager.getGValues(step);

        assertEquals(2, result.size());
        assertThat(result.contains(lowRange), is(true));
        assertThat(result.contains(highRange), is(true));
    }

    @Test
    public void shouldReturnEmptyCollectionForNonParameterizedStep() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);

        final Collection<GValue<?>> result = manager.getGValues(step);

        assertThat(result.isEmpty(), is(true));
    }

    @Test
    public void shouldCheckIfStepIsParameterized() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final StepContract contract = mock(StepContract.class);

        assertThat(manager.isParameterized(step), is(false));

        manager.register(step, contract);
        assertThat(manager.isParameterized(step), is(true));
    }

    @Test
    public void shouldGetAllRegisteredSteps() {
        final GValueManager manager = new GValueManager();
        final Step step1 = mock(Step.class);
        final Step step2 = mock(Step.class);
        final StepContract contract = mock(StepContract.class);

        manager.register(step1, contract);
        manager.register(step2, contract);

        final Set<Step> steps = manager.getSteps();

        assertEquals(2, steps.size());
        assertThat(steps.contains(step1), is(true));
        assertThat(steps.contains(step2), is(true));
    }

    @Test
    public void shouldGetVariableNames() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final TailContract<GValue<Long>> contract = mock(TailContract.class);

        final GValue<Long> limit = GValue.ofLong("limit", 5L);
        when(contract.getLimit()).thenReturn(limit);

        manager.register(step, contract);
        manager.lock();

        final Set<String> variableNames = manager.getVariableNames();

        assertEquals(1, variableNames.size());
        assertThat(variableNames.contains("limit"), is(true));
    }

    @Test
    public void shouldGetAllGValues() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final TailContract<GValue<Long>> contract = mock(TailContract.class);

        final GValue<Long> limit = GValue.ofLong("limit", 5L);
        when(contract.getLimit()).thenReturn(limit);

        manager.register(step, contract);
        manager.lock();

        final Set<GValue> gValues = manager.getGValues();

        assertEquals(1, gValues.size());
        assertThat(gValues.contains(limit), is(true));
    }

    @Test
    public void shouldGatherStepGValues() {
        final GValueManager manager = new GValueManager();
        final Traversal.Admin<?, ?> traversal = mock(Traversal.Admin.class);
        final Step step = mock(Step.class);
        final TailContract<GValue<Long>> contract = mock(TailContract.class);

        final GValue<Long> limit = GValue.ofLong("limit", 5L);
        when(contract.getLimit()).thenReturn(limit);

        manager.register(step, contract);
        when(traversal.getGValueManager()).thenReturn(manager);
        when(traversal.getSteps()).thenReturn(Collections.singletonList(step));

        final Map<Step, Set<GValue>> result = manager.gatherStepGValues(traversal);

        assertEquals(1, result.size());
        assertThat(result.containsKey(step), is(true));
        assertEquals(1, result.get(step).size());
        assertThat(result.get(step).contains(limit), is(true));
    }

    @Test
    public void shouldGatherStepGValuesRecursivelyFromGlobalChildren() {
        final GValueManager manager = new GValueManager();
        final Traversal.Admin<?, ?> parentTraversal = mock(Traversal.Admin.class);
        when(parentTraversal.getGValueManager()).thenReturn(manager);

        final TraversalParent parentStep = mock(TraversalParent.class, withSettings().extraInterfaces(Step.class));
        final Step parentStepAsStep = (Step) parentStep;

        // create global child traversal
        final Traversal.Admin<?, ?> globalChildTraversal = mock(Traversal.Admin.class);
        when(globalChildTraversal.getGValueManager()).thenReturn(manager);

        // create a step for the global child traversal
        final Step globalChildStep = mock(Step.class);
        final TailContract<GValue<Long>> globalChildContract = mock(TailContract.class);
        final GValue<Long> globalChildLimit = GValue.ofLong("globalLimit", 10L);
        when(globalChildContract.getLimit()).thenReturn(globalChildLimit);

        // register the global child step
        manager.register(globalChildStep, globalChildContract);

        // set up the global child traversal
        when(globalChildTraversal.getSteps()).thenReturn(Collections.singletonList(globalChildStep));

        // set up the parent step to return the global child traversal
        when(parentStep.getGlobalChildren()).thenReturn((List) Collections.singletonList(globalChildTraversal));
        when(parentStep.getLocalChildren()).thenReturn((List) Collections.emptyList());

        // set up the parent traversal
        when(parentTraversal.getSteps()).thenReturn(Collections.singletonList(parentStepAsStep));

        final Map<Step, Set<GValue>> result = manager.gatherStepGValues(parentTraversal);

        // Verify the result contains the global child step and its GValue
        assertEquals(1, result.size());
        assertThat(result.containsKey(globalChildStep), is(true));
        assertEquals(1, result.get(globalChildStep).size());
        assertThat(result.get(globalChildStep).contains(globalChildLimit), is(true));
    }

    @Test
    public void shouldGatherStepGValuesRecursivelyFromLocalChildren() {
        final GValueManager manager = new GValueManager();
        final Traversal.Admin<?, ?> parentTraversal = mock(Traversal.Admin.class);
        when(parentTraversal.getGValueManager()).thenReturn(manager);

        // create a step that implements TraversalParent
        final TraversalParent parentStep = mock(TraversalParent.class, withSettings().extraInterfaces(Step.class));
        final Step parentStepAsStep = (Step) parentStep;

        // create local child traversal
        final Traversal.Admin<?, ?> localChildTraversal = mock(Traversal.Admin.class);
        when(localChildTraversal.getGValueManager()).thenReturn(manager);

        // create a step for the local child traversal
        final Step localChildStep = mock(Step.class);
        final TailContract<GValue<Long>> localChildContract = mock(TailContract.class);
        final GValue<Long> localChildLimit = GValue.ofLong("localLimit", 20L);
        when(localChildContract.getLimit()).thenReturn(localChildLimit);

        // register the local child step
        manager.register(localChildStep, localChildContract);

        // set up the local child traversal
        when(localChildTraversal.getSteps()).thenReturn(Collections.singletonList(localChildStep));

        // set up the parent step to return the local child traversal
        when(parentStep.getGlobalChildren()).thenReturn((List) Collections.emptyList());
        when(parentStep.getLocalChildren()).thenReturn((List) Collections.singletonList(localChildTraversal));

        // set up the parent traversal
        when(parentTraversal.getSteps()).thenReturn(Collections.singletonList(parentStepAsStep));

        final Map<Step, Set<GValue>> result = manager.gatherStepGValues(parentTraversal);

        // Verify the result contains the local child step and its GValue
        assertEquals(1, result.size());
        assertThat(result.containsKey(localChildStep), is(true));
        assertEquals(1, result.get(localChildStep).size());
        assertThat(result.get(localChildStep).contains(localChildLimit), is(true));
    }

    @Test
    public void shouldGatherStepGValuesRecursivelyFromBothGlobalAndLocalChildren() {
        final GValueManager manager = new GValueManager();
        final Traversal.Admin<?, ?> parentTraversal = mock(Traversal.Admin.class);
        when(parentTraversal.getGValueManager()).thenReturn(manager);

        // create a step that implements TraversalParent
        final TraversalParent parentStep = mock(TraversalParent.class, withSettings().extraInterfaces(Step.class));
        final Step parentStepAsStep = (Step) parentStep;

        // create global child traversal
        final Traversal.Admin<?, ?> globalChildTraversal = mock(Traversal.Admin.class);
        when(globalChildTraversal.getGValueManager()).thenReturn(manager);

        // create a step for the global child traversal
        final Step globalChildStep = mock(Step.class);
        final TailContract<GValue<Long>> globalChildContract = mock(TailContract.class);
        final GValue<Long> globalChildLimit = GValue.ofLong("globalLimit", 10L);
        when(globalChildContract.getLimit()).thenReturn(globalChildLimit);

        // register the global child step
        manager.register(globalChildStep, globalChildContract);

        // set up the global child traversal
        when(globalChildTraversal.getSteps()).thenReturn(Collections.singletonList(globalChildStep));

        // create local child traversal
        final Traversal.Admin<?, ?> localChildTraversal = mock(Traversal.Admin.class);
        when(localChildTraversal.getGValueManager()).thenReturn(manager);

        // create a step for the local child traversal
        final Step localChildStep = mock(Step.class);
        final TailContract<GValue<Long>> localChildContract = mock(TailContract.class);
        final GValue<Long> localChildLimit = GValue.ofLong("localLimit", 20L);
        when(localChildContract.getLimit()).thenReturn(localChildLimit);

        // register the local child step
        manager.register(localChildStep, localChildContract);

        // set up the local child traversal
        when(localChildTraversal.getSteps()).thenReturn(Collections.singletonList(localChildStep));

        // set up the parent step to return both global and local child traversals
        when(parentStep.getGlobalChildren()).thenReturn((List) Collections.singletonList(globalChildTraversal));
        when(parentStep.getLocalChildren()).thenReturn((List) Collections.singletonList(localChildTraversal));

        // set up the parent traversal
        when(parentTraversal.getSteps()).thenReturn(Collections.singletonList(parentStepAsStep));

        final Map<Step, Set<GValue>> result = manager.gatherStepGValues(parentTraversal);

        // verify the result contains both child steps and their GValues
        assertEquals(2, result.size());
        assertThat(result.containsKey(globalChildStep), is(true));
        assertEquals(1, result.get(globalChildStep).size());
        assertThat(result.get(globalChildStep).contains(globalChildLimit), is(true));
        assertThat(result.containsKey(localChildStep), is(true));
        assertEquals(1, result.get(localChildStep).size());
        assertThat(result.get(localChildStep).contains(localChildLimit), is(true));
    }

    @Test
    public void shouldCheckIfManagerHasStepRegistrations() {
        final GValueManager manager = new GValueManager();

        assertThat(manager.hasStepRegistrations(), is(false));

        final Step step = mock(Step.class);
        final StepContract contract = mock(StepContract.class);
        manager.register(step, contract);

        assertThat(manager.hasStepRegistrations(), is(true));
    }

    @Test
    public void shouldCheckIfManagerHasVariables() {
        final GValueManager manager = new GValueManager();

        assertThat(manager.hasVariables(), is(false));

        final Step step = mock(Step.class);
        final TailContract<GValue<Long>> contract = mock(TailContract.class);
        final GValue<Long> limit = GValue.ofLong("limit", 5L);
        when(contract.getLimit()).thenReturn(limit);

        manager.register(step, contract);
        manager.lock();

        assertThat(manager.hasVariables(), is(true));
    }

    @Test
    public void shouldResetManager() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final StepContract contract = mock(StepContract.class);

        manager.register(step, contract);

        manager.reset();

        assertThat(manager.isParameterized(step), is(false));
        assertThat(manager.getSteps().isEmpty(), is(true));
    }

    @Test
    public void shouldRemoveStep() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);

        final TailContract<GValue<Long>> contract = mock(TailContract.class);
        when(contract.getLimit()).thenReturn(GValue.ofLong(5L)); // Non-variable GValue

        manager.register(step, contract);

        final StepContract removedContract = manager.remove(step);

        assertEquals(contract, removedContract);
        assertThat(manager.isParameterized(step), is(false));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenRemovingStepAfterLock() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);

        final TailContract<GValue<Long>> contract = mock(TailContract.class);
        when(contract.getLimit()).thenReturn(GValue.ofLong(5L));

        manager.register(step, contract);
        manager.lock();

        manager.remove(step);
    }

    @Test
    public void shouldCloneManager() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final StepContract contract = mock(StepContract.class);

        manager.register(step, contract);

        final GValueManager clonedManager = manager.clone();

        assertThat(clonedManager.isParameterized(step), is(true));
        assertEquals(contract, clonedManager.getStepContract(step));
    }

    @Test
    public void shouldMergeHasContainerHolders() {
        final GValueManager manager = new GValueManager();
        final HasContainerHolderStep<Object, Object> sourceStep = mock(HasContainerHolderStep.class);
        final HasContainerHolderStep<Object, Object> targetStep = mock(HasContainerHolderStep.class);

        final HasContainer hasContainer1 = mock(HasContainer.class);
        final HasContainer hasContainer2 = mock(HasContainer.class);

        final List<HasContainer> sourceHasContainers = new ArrayList<>();
        sourceHasContainers.add(hasContainer1);
        sourceHasContainers.add(hasContainer2);

        when(sourceStep.getHasContainers()).thenReturn(sourceHasContainers);
        manager.register(sourceStep, sourceStep);
        manager.register(targetStep, targetStep);

        manager.copyRegistryState(sourceStep, targetStep);

        verify(targetStep).addHasContainer(hasContainer1);
        verify(targetStep).addHasContainer(hasContainer2);
    }

    @Test
    public void shouldGetGValuesFromElementIdsContract() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final ElementIdsContract<GValue<?>> contract = mock(ElementIdsContract.class);

        final GValue<String> id1 = GValue.ofString("var1", "id1");
        final GValue<String> id2 = GValue.ofString("var2", "id2");
        final GValue<String> id3 = GValue.ofString(null, "id3"); // Non-variable GValue
        when(contract.getIds()).thenReturn(new GValue[]{id1, id2, id3});

        manager.register(step, contract);

        final Collection<GValue<?>> result = manager.getGValues(step);

        // Only variable GValues should be extracted
        assertEquals(2, result.size());
        assertThat(result.contains(id1), is(true));
        assertThat(result.contains(id2), is(true));
        assertThat(result.contains(id3), is(false));
    }

    @Test
    public void shouldGetGValuesFromAddVertexContract() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final AddVertexContract<GValue<String>, String, GValue<?>> contract = mock(AddVertexContract.class);

        final GValue<String> label = GValue.ofString("x", "person");
        final GValue<String> name = GValue.ofString("y", "John");
        final GValue<Integer> age = GValue.ofInteger("z", 30);

        final Map<String, GValue<?>> properties = new HashMap<>();
        properties.put("name", name);
        properties.put("age", age);

        when(contract.getLabel()).thenReturn(label);
        when(contract.getProperties()).thenReturn(properties);

        manager.register(step, contract);

        final Collection<GValue<?>> result = manager.getGValues(step);

        assertEquals(3, result.size());
        assertThat(result.contains(label), is(true));
        assertThat(result.contains(name), is(true));
        assertThat(result.contains(age), is(true));
    }

    @Test
    public void shouldGetGValuesFromAddEdgeContract() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final AddEdgeContract<GValue<String>, GValue<Vertex>, GValue<?>, GValue<?>> contract = mock(AddEdgeContract.class);

        final GValue<String> edgeLabel = GValue.ofString("labelVar", "knows");
        final GValue<Vertex> fromVertex = GValue.ofVertex("fromVar", mock(Vertex.class));
        final GValue<Vertex> toVertex = GValue.ofVertex("toVar", mock(Vertex.class));
        final GValue<String> propertyKey = GValue.ofString("keyVar", "core");

        final Map properties = new HashMap<>();
        properties.put("key", propertyKey);

        when(contract.getLabel()).thenReturn(edgeLabel);
        when(contract.getFrom()).thenReturn(fromVertex);
        when(contract.getTo()).thenReturn(toVertex);
        when(contract.getProperties()).thenReturn(properties);

        manager.register(step, contract);

        final Collection<GValue<?>> result = manager.getGValues(step);

        assertEquals(4, result.size());
        assertThat(result.contains(edgeLabel), is(true));
        assertThat(result.contains(fromVertex), is(true));
        assertThat(result.contains(toVertex), is(true));
        assertThat(result.contains(propertyKey), is(true));
    }

    @Test
    public void shouldGetGValuesFromAddPropertyContract() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final AddPropertyContract<GValue<String>, GValue<?>> contract = mock(AddPropertyContract.class);

        final GValue propertyValue = GValue.ofString("valueVar", "propertyValue");
        final GValue<Integer> extraProperty = GValue.ofInteger("extraVar", 100);

        final Map extraProperties = new HashMap<>();
        extraProperties.put("extraKey", extraProperty);

        when(contract.getValue()).thenReturn(propertyValue);
        when(contract.getProperties()).thenReturn(extraProperties);

        manager.register(step, contract);

        final Collection<GValue<?>> result = manager.getGValues(step);

        assertEquals(2, result.size());
        assertThat(result.contains(propertyValue), is(true));
        assertThat(result.contains(extraProperty), is(true));
    }

    @Test
    public void shouldGetGValuesFromMergeElementContract() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final MergeElementContract<GValue<Map<?, ?>>> contract = mock(MergeElementContract.class);

        final GValue<Map<?, ?>> mergeMap = GValue.ofMap("mergeVar", new HashMap<>());
        final GValue<Map<?, ?>> onCreateMap = GValue.ofMap("createVar", new HashMap<>());
        final GValue<Map<?, ?>> onMatchMap = GValue.ofMap("matchVar", new HashMap<>());

        when(contract.getMergeMap()).thenReturn(mergeMap);
        when(contract.getOnCreateMap()).thenReturn(onCreateMap);
        when(contract.getOnMatchMap()).thenReturn(onMatchMap);

        manager.register(step, contract);

        final Collection<GValue<?>> result = manager.getGValues(step);

        assertEquals(3, result.size());
        assertThat(result.contains(mergeMap), is(true));
        assertThat(result.contains(onCreateMap), is(true));
        assertThat(result.contains(onMatchMap), is(true));
    }

    @Test
    public void shouldGetGValuesFromEdgeLabelContract() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final EdgeLabelContract<GValue<String>> contract = mock(EdgeLabelContract.class);

        final GValue<String> namedLabel = GValue.ofString("labelVar", "relationship");
        final GValue<String> unnamedLabel = GValue.ofString(null, "meta");

        when(contract.getEdgeLabels()).thenReturn(new GValue[]{namedLabel, unnamedLabel});

        manager.register(step, contract);

        final Collection<GValue<?>> result = manager.getGValues(step);

        assertEquals(1, result.size());
        assertThat(result.contains(namedLabel), is(true));
        assertThat(result.contains(unnamedLabel), is(false));
    }

    @Test
    public void shouldGetGValuesFromCallContract() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final CallContract<GValue<Map<?, ?>>> contract = mock(CallContract.class);

        final GValue<Map<?, ?>> staticParams = GValue.ofMap("paramsVar", new HashMap<>());

        when(contract.getStaticParams()).thenReturn(staticParams);

        manager.register(step, contract);

        final Collection<GValue<?>> result = manager.getGValues(step);

        assertEquals(1, result.size());
        assertThat(result.contains(staticParams), is(true));
    }

    @Test
    public void shouldGetGValuesFromPredicateContract() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);
        final GValue<String> predicateValue = GValue.ofString("predVar", "test");
        final PredicateContract contract = () -> new P(Compare.eq, predicateValue);

        manager.register(step, contract);

        final Collection<GValue<?>> result = manager.getGValues(step);

        assertEquals(1, result.size());
        assertThat(result.contains(predicateValue), is(true));
    }

    @Test
    public void shouldGetGValuesFromHasContainerHolder() {
        final GValueManager manager = new GValueManager();
        final Step step = mock(Step.class);

        // create a real HasContainerHolder implementation
        final GValue<String> predicateValue = GValue.ofString("predVar", "test");
        final P<?> predicate = new P(Compare.eq, predicateValue);

        final HasContainerHolder contract = new HasContainerHolder() {
            @Override
            public List<HasContainer> getHasContainers() {
                return Collections.emptyList();
            }

            @Override
            public void addHasContainer(final HasContainer hasContainer) {
                // Not needed for this test
            }

            @Override
            public Collection<P<?>> getPredicates() {
                return Collections.singletonList(predicate);
            }
        };

        manager.register(step, contract);

        final Collection<GValue<?>> result = manager.getGValues(step);

        assertEquals(1, result.size());
        assertThat(result.contains(predicateValue), is(true));
    }

    /**
     * Interface that combines Step and HasContainerHolder for testing.
     */
    private interface HasContainerHolderStep<S, E> extends Step<S, E>, HasContainerHolder {
    }
}
