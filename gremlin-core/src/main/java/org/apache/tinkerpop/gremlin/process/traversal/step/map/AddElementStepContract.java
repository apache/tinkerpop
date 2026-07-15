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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.PropertiesHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface AddElementStepContract<S, E> extends Step<S, E>, PropertiesHolder, TraversalParent, Scoping, Configuring {

    /**
     * Concrete implementations of this contract that can be referenced as TinkerPop implementations.
     */
    List<Class<? extends Step>> CONCRETE_STEPS = Stream.of(AddVertexStepContract.CONCRETE_STEPS, AddEdgeStepContract.CONCRETE_STEPS)
            .flatMap(List::stream).collect(Collectors.toList());

    /**
     * Gets the label(s) assigned to the element produced by this step. Returns a single value, or a
     * {@code Set} when multiple labels were specified (e.g. {@code addV("a", "b")}). Each individual label
     * value will either be a {@code String} or {@link Traversal} depending on how it was initially set. Callers
     * should not assume the type is {@code String} and should check with {@code instanceof} before casting.
     *
     * @return a {@code String}, {@code Traversal}, or a {@code Set} containing either
     */
    Object getLabel();

    default Object getLabelWithGValue() {
        return getLabel();
    }

    /**
     * Sets the label(s) to assign to the element produced by this step. Accepts a single {@code String},
     * {@code GValue<String>}, or {@code Traversal} resolving to a {@code String}, or a {@code Collection<Object>}
     * of two or more such values. Should behave identically to specifying the same value at construction time.
     * Implementations may restrict calling this method more than once at their own discretion.
     *
     * @param label a {@code String}, {@code GValue<String>}, {@code Traversal}, or {@code Collection<Object>}
     */
    void setLabel(Object label);

    Object getElementId();

    default Object getElementIdWithGValue() {
        return getElementId();
    }

    void setElementId(Object elementId);

    boolean removeElementId();

    @Override
    default HashSet<PopInstruction> getPopInstructions() {
        return TraversalParent.super.getPopInstructions();
    }
}
