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
 * Note that the manager can be locked, at which point it becomes immutable, and any attempt to modify it will result
 * in an exception.
 */
public final class GValueManager implements Serializable, Cloneable {

    private final Map<String, GValue<?>> gValueRegistry = new HashMap<>();
    private final Map<String, GValue<?>> pinnedGValues = new HashMap<>();

    public GValueManager() {
        this(Collections.EMPTY_MAP, Collections.EMPTY_MAP);
    }

    private GValueManager(final Map<String, GValue<?>> gValueRegistry, final Map<String, GValue<?>> pinnedGValues) {
        this.gValueRegistry.putAll(gValueRegistry);
        this.pinnedGValues.putAll(pinnedGValues);
    }

    /**
     * Merges the internal registries of the current {@code GValueManager} into another {@code GValueManager}.
     * Transfers the state of the {@code gValueRegistry} and {@code stepRegistry} from the current instance to the
     * specified target instance.
     *
     * @param other the target {@code GValueManager} into which the current instance's registries will be merged
     */
    public void mergeInto(final GValueManager other) {
        //TODO needs test
        other.register(gValueRegistry.values());
        other.pinGValues(pinnedGValues.values());
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
        Set<String> variableNames = new HashSet<>();
        variableNames.addAll(gValueRegistry.keySet());
        variableNames.removeAll(pinnedGValues.keySet());
        return Collections.unmodifiableSet(variableNames);
    }

    /**
     * Gets the set of variable names used in this traversal which have not been pinned to specific values.
     */
    public Set<String> getPinnedVariableNames() {
        return Collections.unmodifiableSet(pinnedGValues.keySet());
    }

    /**
     * Gets the set of {@link GValue} objects used in this traversal.
     */
    public Set<GValue<?>> getGValues() {
        return Collections.unmodifiableSet(new HashSet(gValueRegistry.values()));
    }

    /**
     * Gets the set of pinned GValues used in this traversal.
     */
    public Set<GValue<?>> getPinnedGValues() {
        return Collections.unmodifiableSet(new HashSet(pinnedGValues.values()));
    }

    /**
     * Determines whether the manager has step registrations.
     */
    public boolean hasVariables() {
        return !getVariableNames().isEmpty();
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
     * @throws IllegalStateException if the manager is locked
     */
    public boolean pinVariable(final String name) {
        if (name == null || !gValueRegistry.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Variable '%s' does not exist", name));
        }
        return pinnedGValues.put(name, gValueRegistry.get(name)) == null;
    }

    @Override
    public GValueManager clone() {
        return new GValueManager(gValueRegistry, pinnedGValues);
    }

    /**
     * Pin a collection of {@link GValue} instances from the internal registry. Iteratively processes each
     * {@link GValue} to pin it to its current value.
     */
    public void pinGValues(final Collection<GValue<?>> gValues) { //TODO:: should there be a return value?
        gValues.stream().filter(v -> v != null && v.isVariable()).forEach(v -> pinVariable(v.getName()));
    }

    public void register(GValue<?> gValue) {
        if (gValue.isVariable()) {
            if (gValueRegistry.containsKey(gValue.getName()) && !gValueRegistry.get(gValue.getName()).equals(gValue)) {
                throw new IllegalArgumentException(String.format("Unable to register both %s and %s in a single traversal", gValueRegistry.get(gValue.getName()), gValue));
            }
            gValueRegistry.put(gValue.getName(), gValue);
        }
    }

    public void register(Collection<GValue<?>> gValues) {
        for (GValue<?> gValue : gValues) {
            register(gValue);
        }
    }

    public void updateVariable(String name, Object value) {
        if (!getVariableNames().contains(name)) {
            throw new IllegalArgumentException(String.format("Unable to update variable '%s' as it has not been registered in this traversal", name));
        } else if (pinnedGValues.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Unable to update variable '%s' as it is already pinned", name));
        }
        gValueRegistry.put(name, GValue.of(name, value));
    }
}
