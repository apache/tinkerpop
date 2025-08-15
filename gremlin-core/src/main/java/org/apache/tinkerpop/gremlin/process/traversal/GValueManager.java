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

import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The {@code GValueManager} class is responsible for managing the state of {@link GValue} instances and their
 * associations with `Step` objects in a traversal. This class ensures that `GValue` instances are properly extracted
 * and stored in a registry, allowing for dynamic query optimizations and state management during traversal execution.
 */
public class GValueManager implements Serializable, Cloneable {

    private final Map<String, GValue<?>> gValueRegistry = new HashMap<>();
    /**
     * Tracks pinned values for faster lookup.
     */
    private final Set<String> pinnedGValues = new HashSet<>();

    /**
     * Creates a new empty GValueManager.
     */
    public GValueManager() {
        this(Collections.emptyMap(), Collections.emptySet());
    }

    private GValueManager(final Map<String, GValue<?>> gValueRegistry, final Set<String> pinnedGValues) {
        this.gValueRegistry.putAll(gValueRegistry);
        this.pinnedGValues.addAll(pinnedGValues);
    }

    /**
     * Merges the internal registries of the current {@code GValueManager} into another {@code GValueManager}.
     * Transfers the state of the {@code gValueRegistry} and {@code stepRegistry} from the current instance to the
     * specified target instance.
     *
     * @param other the target {@code GValueManager} into which the current instance's registries will be merged
     */
    public void mergeInto(final GValueManager other) {
        other.register(gValueRegistry.values());
        other.pinGValues(pinnedGValues);
    }

    /**
     * Gets the set of variable names used in this traversal.
     */
    public Set<String> getVariableNames() {
        return Collections.unmodifiableSet(gValueRegistry.keySet());
    }

    /**
     * Gets the set of variable names used in this traversal which have not been pinned to specific values.
     */
    public Set<String> getUnpinnedVariableNames() {
        final Set<String> variableNames = new HashSet<>(gValueRegistry.keySet());
        variableNames.removeAll(pinnedGValues);
        return Collections.unmodifiableSet(variableNames);
    }

    /**
     * Gets the set of variable names used in this traversal which have not been pinned to specific values.
     */
    public Set<String> getPinnedVariableNames() {
        return Collections.unmodifiableSet(pinnedGValues);
    }

    /**
     * Gets the set of {@link GValue} objects used in this traversal.
     */
    public Set<GValue<?>> getGValues() {
        return Set.copyOf(gValueRegistry.values());
    }

    /**
     * Gets the set of pinned GValues used in this traversal.
     */
    public Set<GValue<?>> getPinnedGValues() {
        return pinnedGValues.stream().map(gValueRegistry::get).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Determines whether the manager has step registrations.
     */
    public boolean hasVariables() {
        return !gValueRegistry.isEmpty();
    }

    /**
     * Determines whether the manager has unpinned GValues.
     */
    public boolean hasUnpinnedVariables() {
        return !getUnpinnedVariableNames().isEmpty();
    }

    /**
     * Pins any GValue with the provided name. This should be called anytime an optimization
     * alters the Traversal based on the current value of a GValue. This indicates that the resulting traversal is only
     * valid for this specific value of a GValue, and is not generalizable to any parameter value.
     *
     * @param name the name of the GValue to be pinned
     * @return true if name was previously unpinned, false otherwise.
     * @throws IllegalArgumentException if no GValue of the given name is registered
     */
    public boolean pinVariable(final String name) {
        if (name == null || !gValueRegistry.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Unable to pin variable '%s' because it is not registered", name));
        }
        return pinnedGValues.add(name);
    }

    /**
     * Creates a deep copy of this GValueManager with cloned GValue instances.
     *
     * @return a new GValueManager instance with cloned state
     * @throws CloneNotSupportedException if cloning fails
     */
    @Override
    public GValueManager clone() throws CloneNotSupportedException {
        final Map<String, GValue<?>> clonedRegistry = new HashMap<>();
        for (final Map.Entry<String, GValue<?>> entry : gValueRegistry.entrySet()) {
            // clone each gValue
            clonedRegistry.put(entry.getKey(), entry.getValue().clone());
        }
        return new GValueManager(clonedRegistry, new HashSet<>(pinnedGValues));
    }

    /**
     * Pin a collection of {@link GValue} by name from the internal registry. Iteratively processes each
     * {@link GValue} name to pin it to its current value.
     */
    public void pinGValues(final Set<String> names) {
        names.stream().filter(Objects::nonNull).forEach(this::pinVariable);
    }

    /**
     * Pin a collection of {@link GValue} instances from the internal registry. Iteratively processes each
     * {@link GValue} to pin it to its current value.
     */
    public void pinGValues(final Collection<GValue<?>> gValues) {
        pinGValues(gValues.stream().filter(v -> v != null && v.isVariable()).map(GValue::getName).collect(Collectors.toSet()));
    }

    /**
     * Registers a GValue in the internal registry if it represents a variable. Non-variables are not registered.
     *
     * @param gValue the GValue to register
     * @throws IllegalArgumentException if a different GValue with the same name is already registered
     */
    public void register(final GValue<?> gValue) {
        if (gValue.isVariable()) {
            if (gValueRegistry.containsKey(gValue.getName()) && !gValueRegistry.get(gValue.getName()).equals(gValue)) {
                throw new IllegalArgumentException(String.format("Unable to register both %s and %s in a single traversal", gValueRegistry.get(gValue.getName()), gValue));
            }
            gValueRegistry.put(gValue.getName(), gValue);
        }
    }

    /**
     * Registers a collection of GValues in the internal registry. Non-variables are not registered.
     *
     * @param gValues the collection of GValues to register
     */
    public void register(final Collection<GValue<?>> gValues) {
        gValues.forEach(this::register);
    }

    /**
     * Updates the value of a registered variable.
     *
     * @param name the name of the variable to update
     * @param value the new value for the variable
     * @throws IllegalArgumentException if the variable is not registered or is already pinned
     */
    public void updateVariable(final String name, final Object value) {
        if (!gValueRegistry.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Unable to update variable '%s' as it has not been registered in this traversal", name));
        } else if (pinnedGValues.contains(name)) {
            throw new IllegalArgumentException(String.format("Unable to update variable '%s' as it is already pinned", name));
        }
        gValueRegistry.put(name, GValue.of(name, value));
    }
}
