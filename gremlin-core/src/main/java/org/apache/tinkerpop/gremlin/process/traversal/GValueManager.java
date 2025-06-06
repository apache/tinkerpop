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

import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddEdgeContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddVertexContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.CallContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.ElementIdsContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.EdgeLabelContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddPropertyContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.MergeElementContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.PredicateContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.RangeContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.StepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.TailContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The {@code GValueManager} class is responsible for managing the state of {@link GValue} instances and their
 * associations with `Step` objects in a traversal. This class ensures that `GValue` instances are properly extracted
 * and stored in a registry, allowing for  dynamic query optimizations and state management during traversal execution.
 * Note that the manager can be locked, at which point it becomes immutable, and any attempt to modify it will result
 * in an exception.
 */
public final class GValueManager implements Serializable, Cloneable {

    private boolean locked = false;
    private final Map<String, GValue> gValueRegistry = new IdentityHashMap();
    private final Map<Step, StepContract> stepRegistry = new IdentityHashMap();

    public GValueManager() {
        this(Collections.EMPTY_MAP, Collections.EMPTY_MAP);
    }

    private GValueManager(final Map<String, GValue> gValueRegistry, final Map<Step, StepContract> stepRegistry) {
        this.gValueRegistry.putAll(gValueRegistry);
        this.stepRegistry.putAll(stepRegistry);
    }

    /**
     * Register a step with any {@link StepContract}.
     */
    public void register(final Step step, final StepContract contract) {
        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();
        stepRegistry.put(step, contract);
    }

    /**
     * Locks the current state of the manager, ensuring that no further modifications can be made.
     * <p/>
     * This method processes all registered steps and predicates by extracting relevant {@link GValue}
     * instances from their associated contracts or predicates and adding them to the internal registry.
     * If the manager is already locked, invoking this method has no effect.
     * <p/>
     * During the locking process, the manager iterates over the registered steps and applies the appropriate
     * extraction logic for each contract type. If a contract type is found that is not supported, an
     * {@link IllegalArgumentException} is thrown.
     */
    public synchronized void lock() {
        // can only lock once so if already locked, just return.
        if (locked) return;
        locked = true;

        for (StepContract contract : stepRegistry.values()) {
            registerGValues(extractGValues(contract));
        }
    }

    /**
     * Determines if the manager is locked or not.
     */
    public boolean isLocked() {
        return locked;
    }

    public void mergeInto(final GValueManager other) {
        //TODO, how to proceed when locked? Often end up here with locked traversals in OLAP as Traversals are cloned after strategies have finished running.
//        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();

        //TODO deal with conflicts
        other.gValueRegistry.putAll(gValueRegistry);
        other.stepRegistry.putAll(stepRegistry);
    }

    /**
     * Copy the registry state from one step to another.
     */
    public <S, E> void copyRegistryState(final Step<S,E> sourceStep, final Step<S,E> targetStep) {
        if (this.locked) {
            throw Traversal.Exceptions.traversalIsLocked();
        }
        if (stepRegistry.containsKey(sourceStep)) {
            // todo: oh boy - should has stuff even be handled this way?
            // todo: is it ok that the StepContract can be tied to two different steps? like, can happen with Traversal.clone() where you gotta copy

            // gotta merge HasContainerHolder since it already exists
            if (targetStep instanceof HasContainerHolder && stepRegistry.containsKey(targetStep) &&
                    sourceStep instanceof HasContainerHolder) {
                final HasContainerHolder targetHasContainer = (HasContainerHolder) stepRegistry.get(targetStep);
                final HasContainerHolder sourceHasContainer = (HasContainerHolder) stepRegistry.get(sourceStep);
                sourceHasContainer.getHasContainers().forEach(targetHasContainer::addHasContainer);
                stepRegistry.put(targetStep, targetHasContainer);
            } else {
                stepRegistry.put(targetStep, stepRegistry.get(sourceStep));
            }
        }
    }

    /**
     * Retrieves the {@link StepContract} associated with the given {@link Step}.  This method uses the internal step
     * registry to fetch the associated contract. It is expected that the step has been previously registered with a
     * corresponding contract.
     */
    public <T extends StepContract> T getStepContract(final Step step) {
        return (T) stepRegistry.get(step);
    }

    public Collection<GValue<?>> getGValues(final Step step) {
        if (!isParameterized(step)) return Collections.emptyList();
        return extractGValues(getStepContract(step));
    }

    /**
     * Determines whether the given step is parameterized by checking its presence
     * in the step registry or the predicate registry.
     *
     * @param step the {@link Step} to be checked
     * @return {@code true} if the step is present in the step registry or predicate registry,
     *         {@code false} otherwise
     */
    public <S> boolean isParameterized(final Step step) {
        return this.stepRegistry.containsKey(step);
    }

    public Set<Step> getSteps() {
        return Collections.unmodifiableSet(stepRegistry.keySet());
    }

    /**
     * Gets the set of variable names used in this traversal. Note that this set won't be consistent until
     * {@link #lock()} is called first. Calls to this method prior to locking will force iteration through traversal
     * steps to real-time gather then variable names.
     */
    public Set<String> variableNames() {
        if (locked) {
            return Collections.unmodifiableSet(gValueRegistry.keySet());
        } else {
            return Collections.unmodifiableSet(gValues().stream().map(GValue::getName).collect(Collectors.toSet()));
        }
    }

    /**
     * Gets the set of {@link GValue} objects used in this traversal. Note that this set won't be consistent until
     * {@link #lock()} is called first. Calls to this method prior to locking will force iteration through traversal
     * steps to real-time gather them.
     */
    public Set<GValue> gValues() {
        if (locked) {
            return Collections.unmodifiableSet(new HashSet<>(gValueRegistry.values()));
        } else {
            final Set<GValue> gvalues = new HashSet<>();
            for (StepContract contract : stepRegistry.values()) {
                extractGValues(contract).stream().
                        filter(GValue::isVariable).forEach(gvalues::add);
            }
            return Collections.unmodifiableSet(gvalues);
        }
    }

    /**
     * Recursively collect {@link Step} to {@link GValue} mappings from the specified traversal and its children.
     */
    public Map<Step, Set<GValue>> gatherStepGValues(final Traversal.Admin<?, ?> traversal) {
        return gatherStepGValues(traversal, null);
    }

    private Map<Step, Set<GValue>> gatherStepGValues(final Traversal.Admin<?, ?> traversal, final Map<Step, Set<GValue>> result) {
        final GValueManager manager = traversal.getGValueManager();
        final Map<Step, Set<GValue>> r = null == result ? new HashMap<>() : result;

        for (Step step : traversal.getSteps()) {
            if (manager.isParameterized(step)) {
                final StepContract contract = manager.getStepContract(step);
                r.put(step, new HashSet<>(extractGValues(contract)));
            }

            if (step instanceof TraversalParent) {
                TraversalParent parent = (TraversalParent) step;

                // Process global children
                for (Traversal.Admin<?, ?> child : parent.getGlobalChildren()) {
                    gatherStepGValues(child, r);
                }

                // Process local children
                for (Traversal.Admin<?, ?> child : parent.getLocalChildren()) {
                    gatherStepGValues(child, r);
                }
            }
        }

        return r;
    }

    /**
     * Determines whether the manager has step registrations.
     */
    public boolean hasStepRegistrations() {
        return stepRegistry.isEmpty();
    }

    /**
     * Determines whether the manager has step registrations. This call is not consistent until {@link #lock()} is
     * called.
     */
    public boolean hasVariables() {
        return variableNames().isEmpty();
    }

    /**
     * Delete all data
     */
    public void reset() {
        stepRegistry.clear();
        gValueRegistry.clear();
    }

    /**
     * Removes the given {@link Step} from the registry.
     * If the manager is locked, the operation is not permitted and an exception is thrown.
     *
     * @param step the {@link Step} to be removed from the manager
     * @throws IllegalStateException if the manager is locked
     */
    public void remove(final Step step) {
        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();
        final StepContract removedContract = stepRegistry.remove(step);

        if (removedContract != null) {
            removeGValues(extractGValues(removedContract));
        }
    }

    /**
     * Removes all potential step contracts for a particular traversal. This is not a recursive operation, so use
     * {@link #removeAllRecursively(Traversal.Admin)} for that case.
     */
    public void removeAll(final Traversal.Admin<?, ?> traversal) {
        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();
        traversal.getSteps().forEach(this::remove);
    }

    /**
     * Removes all potential step contracts for a particular traversal. This is a recursive operation and will examine
     * all steps in this traversal as well as associated child traversals.
     */
    public void removeAllRecursively(final Traversal.Admin<?, ?> traversal) {
        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();

        for (Step step : traversal.getSteps()) {
            this.remove(step);
            if (step instanceof TraversalParent) {
                TraversalParent parent = (TraversalParent) step;
                for (Traversal.Admin<?, ?> child : parent.getGlobalChildren()) {
                    removeAllRecursively(child);
                }
                for (Traversal.Admin<?, ?> child : parent.getLocalChildren()) {
                    removeAllRecursively(child);
                }
            }
        }
    }

    private Collection<GValue<?>> extractGValues(final StepContract contract) {
        if (contract instanceof RangeContract) {
            return extractGValue((RangeContract<GValue<Long>>) contract);
        } else if (contract instanceof TailContract) {
            return extractGValue((TailContract<GValue<Long>>) contract);
        } else if (contract instanceof MergeElementContract) {
            return extractGValue((MergeElementContract<GValue<Map<?, ?>>> ) contract);
        } else if (contract instanceof ElementIdsContract) {
            return extractGValue((ElementIdsContract<GValue<?>>) contract);
        } else if (contract instanceof AddVertexContract) {
            return extractGValue((AddVertexContract<GValue<String>, ?, GValue<?>>) contract);
        } else if (contract instanceof AddEdgeContract) {
            return extractGValue((AddEdgeContract<GValue<String>, GValue<Vertex>, ?, GValue<?>>) contract);
        } else if (contract instanceof AddPropertyContract) {
            return extractGValue((AddPropertyContract<?, GValue<?>>) contract);
        } else if (contract instanceof EdgeLabelContract) {
            return extractGValue((EdgeLabelContract<GValue<String>>) contract);
        } else if (contract instanceof CallContract) {
            return extractGValue((CallContract<GValue<Map<?, ?>>>) contract);
        } else if (contract instanceof PredicateContract) {
            return extractGValue((PredicateContract) contract);
        } else if (contract instanceof HasContainerHolder) {
            return extractGValue((HasContainerHolder) contract);
        } else {
            throw new IllegalArgumentException("Unsupported StepContract type: " + contract.getClass().getName());
        }
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final RangeContract<GValue<Long>> contract) {
        final Collection<GValue<?>> results = new ArrayList();
        if (contract.getHighRange().isVariable()) {
            results.add(contract.getHighRange());
        }
        if (contract.getLowRange().isVariable()) {
            results.add(contract.getLowRange());
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final TailContract<GValue<Long>> contract) {
        if (contract.getLimit().getName() != null) {
            return Collections.singletonList(contract.getLimit());
        }
        return Collections.emptyList();
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final MergeElementContract<GValue<Map<?,?>>> contract) {
        final Collection<GValue<?>> results = new ArrayList();
        if (contract.getMergeMap() != null) {
            results.add(contract.getMergeMap());
        }
        if (contract.getOnCreateMap() != null) {
            results.add(contract.getOnCreateMap());
        }
        if (contract.getOnMatchMap() != null) {
            results.add(contract.getOnMatchMap());
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final ElementIdsContract<GValue<?>> contract) {
        final Collection<GValue<?>> results = new ArrayList();
        for (GValue gValue: contract.getIds()) {
            if (gValue.isVariable()) {
                results.add(gValue);
            }
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final AddVertexContract<GValue<String>, ?, GValue<?>> contract) {
        final Collection<GValue<?>> results = new ArrayList();
        if (contract.getLabel() != null) {
            results.add(contract.getLabel());
        }
        for (GValue<?> value : contract.getProperties().values()) {
            results.add(value);
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final AddEdgeContract<GValue<String>, GValue<Vertex>, ?, GValue<?>> contract) {
        final Collection<GValue<?>> results = new ArrayList();
        if (contract.getLabel() != null) {
            results.add(contract.getLabel());
        }
        if (contract.getFrom() != null) {
            results.add(contract.getFrom());
        }
        if (contract.getTo() != null) {
            results.add(contract.getTo());
        }
        if (contract.getProperties() != null) {
            for (GValue<?> value : contract.getProperties().values()) {
                results.add(value);
            }
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final AddPropertyContract<?, GValue<?>> contract) {
        final Collection<GValue<?>> results = new ArrayList();
        results.add(contract.getValue());
        for (GValue<?> value : contract.getProperties().values()) {
            results.add(value);
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final EdgeLabelContract<GValue<String>> contract) {
        final Collection<GValue<?>> results = new ArrayList();
        for (GValue gValue: contract.getEdgeLabels()) {
            if (gValue.getName() != null) {
                results.add(gValue);
            }
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final CallContract<GValue<Map<?,?>>> contract) {
        final Collection<GValue<?>> results = new ArrayList();
        if (contract.getStaticParams().getName() != null) {
            results.add(contract.getStaticParams());
        }
        return results;
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final PredicateContract contract) {
        if (contract.getPredicate() != null) {
            return contract.getPredicate().getGValueRegistry().getGValues();
        }
        return Collections.emptyList();
    }

    /**
     * Extract {@link GValue} instances from contracts to the registry after {@link #lock()} has been called on the
     * manager.
     */
    private Collection<GValue<?>> extractGValue(final HasContainerHolder contract) {
        final Collection<GValue<?>> results = new ArrayList();
        for (P<?> predicate : contract.getPredicates()) {
            if (predicate != null) {
                results.addAll(predicate.getGValueRegistry().getGValues());
            }
        }
        return results;
    }

    private void registerGValues(final Collection<GValue<?>> gValues) {
        for (GValue<?> gValue : gValues) {
            if (gValue.isVariable()) {
                gValueRegistry.put(gValue.getName(), gValue);
            }
        }
    }

    private void removeGValues(final Collection<GValue<?>> gValues) {
        for (GValue<?> gValue : gValues) {
            gValueRegistry.remove(gValue.getName(), gValue);
            // TODO:: cascade to other steps
        }
    }

    @Override
    public GValueManager clone() {
        return new GValueManager(gValueRegistry, stepRegistry);
    }
}
