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
package org.apache.tinkerpop.gremlin.process.traversal.traverser;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.map.ReferenceMap;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.LabelledCounter;

import static org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement.NESTED_LOOP;
import static org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement.SINGLE_LOOP;

/**
 * Intermediate helper interface for {@link Traverser}s that support loops. Implementations are expected to override 
 * either the single or nested loop functions depending on the type of loop {@link TraverserRequirement} they are using.
 * <p>
 * Note: it would be nice to separate the {@link TraverserRequirement#SINGLE_LOOP} vs {@link TraverserRequirement#NESTED_LOOP}
 * logic into different traverser interfaces in the future however the current {@link Traverser} class hierarchy makes
 * that difficult as {@link Traverser}s that support nested loops extend from {@link Traverser}s that support single
 * loops but override all single loop logic with nested loop logic.
 */
public interface NL_SL_Traverser<T> extends Traverser.Admin<T> {
    /**
     * @return the supported loop type - either {@link TraverserRequirement#NESTED_LOOP} or {@link TraverserRequirement#SINGLE_LOOP}
     */
    TraverserRequirement getLoopRequirement();

    // NESTED LOOPS

    /**
     * Override this method to support nested loops.
     *
     * @return the {@link Stack} of {@link LabelledCounter} which holds the nested loops.
     */
    default Stack<LabelledCounter> getNestedLoops() {
        throw new UnsupportedOperationException();
    }

    /**
     * Override this method to support nested loops.
     *
     * @return the {@link ReferenceMap} where key=loop name and value=reference to the {@link LabelledCounter}.
     */
    default ReferenceMap<String,Object> getNestedLoopNames() {
        throw new UnsupportedOperationException();
    }

    // SINGLE LOOPS

    /**
     * Override this method to support single loop.
     *
     * @return the single loop count.
     */
    default short getSingleLoopCount() {
        throw new UnsupportedOperationException();
    }

    /**
     * Override this method to support single loop. Sets the single loop count.
     */
    default void setSingleLoopCount(short loops) {
        throw new UnsupportedOperationException();
    }

    /**
     * Override this method to support single loop.
     *
     * @return the single loop name.
     */
    default String getSingleLoopName() {
        throw new UnsupportedOperationException();
    }

    /**
     * Override this method to support single loop. Sets the single loop name.
     */
    default void setSingleLoopName(final String loopName) {
        throw new UnsupportedOperationException();
    }

    /**
     * Override this method to support single loop.
     *
     * @return the single loop step label.
     */
    default String getSingleLoopStepLabel() {
        throw new UnsupportedOperationException();
    }

    /**
     * Override this method to support single loop. Sets the single loop step label.
     */
    default void setSingleLoopStepLabel(final String stepLabel) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void incrLoops() {
        if (NESTED_LOOP.equals(getValidLoopRequirement())) {
            getNestedLoops().peek().increment();
        } else {
            setSingleLoopCount((short) (getSingleLoopCount() + 1));
        }
    }

    /**
     * @return the current loop count for single loops or the latest loop count for nested loops.
     */
    @Override
    default int loops() {
        return loops(null);
    }

    /**
     * Obtain a loop count for a named loop or for the current loop count for single loops or the latest loop count for nested loops.
     *
     * @param loopName the optional name applied to the loop
     * @return the count for the loop identified by the given loopName or current loop count for single loops or latest loop for nested loops.
     */
    @Override
    default int loops(final String loopName) {
        if (NESTED_LOOP.equals(getValidLoopRequirement())) {
            return getNestedLoopCount(loopName);
        } else if (loopName == null ||
                Objects.equals(loopName, getSingleLoopName()) ||
                Objects.equals(loopName, getSingleLoopStepLabel())) {
            return getSingleLoopCount();
        }
        throw new IllegalArgumentException("Single loop name not defined: " + loopName);
    }

    /**
     * Initialises loops depending on the {@link #getLoopRequirement()} loop type.
     *
     * @param stepLabel the label of the step initialising the loops.
     * @param loopName  the optional user defined name for referencing the loop counter
     */
    @Override
    default void initialiseLoops(final String stepLabel, final String loopName) {
        if (NESTED_LOOP.equals(getValidLoopRequirement())) {
            initialiseNestedLoops(stepLabel, loopName);
        } else {
            setSingleLoopStepLabel(stepLabel);
            setSingleLoopName(loopName);
        }
    }

    /**
     * Resets loops depending on the {@link #getLoopRequirement()} loop type.
     */
    @Override
    default void resetLoops() {
        if (NESTED_LOOP.equals(getValidLoopRequirement())) {
            getNestedLoops().pop();
        } else {
            setSingleLoopCount((short) 0);
        }
    }

    /**
     * @return the loop names that can be used to reference loops.
     */
    @Override
    default Set<String> getLoopNames() {
        if (NESTED_LOOP.equals(getValidLoopRequirement())) {
            return getNestedLoopNames().keySet();
        } else {
            return Stream.of(getSingleLoopName(), getSingleLoopStepLabel())
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
        }
    }

    default TraverserRequirement getValidLoopRequirement() {
        final TraverserRequirement loopRequirement = getLoopRequirement();
        if (!SINGLE_LOOP.equals(loopRequirement) && !NESTED_LOOP.equals(loopRequirement)) {
            throw new IllegalStateException("Invalid loop TraverserRequirement " + loopRequirement);
        }
        return loopRequirement;
    }

    default void initialiseNestedLoops(final String stepLabel, final String loopName) {
        final Stack<LabelledCounter> nestedLoops = getNestedLoops();
        final ReferenceMap<String, Object> loopNames = getNestedLoopNames();
        if (nestedLoops.empty() || !nestedLoops.peek().hasLabel(stepLabel)) {
            final LabelledCounter lc = new LabelledCounter(stepLabel, (short) 0);
            nestedLoops.push(lc);
            loopNames.put(stepLabel, lc);
            if (loopName != null && !loopName.equals(stepLabel)) {
                loopNames.put(loopName, lc);
            }
        }
    }

    default int getNestedLoopCount(final String loopName) {
        if (loopName == null) {
            return getNestedLoops().peek().count();
        } else if (getNestedLoopNames().containsKey(loopName)) {
            return ((LabelledCounter) getNestedLoopNames().get(loopName)).count();
        } else {
            throw new IllegalArgumentException("Nested loop name not defined: " + loopName);
        }
    }

    default void copyNestedLoops(final Stack<LabelledCounter> targetNestedLoops, final ReferenceMap<String, Object> targetLoopNames) {
        final Map<LabelledCounter, LabelledCounter> counterMapping = new HashMap<>();

        for (LabelledCounter original : getNestedLoops()) {
            final LabelledCounter cloned = (LabelledCounter) original.clone();
            targetNestedLoops.push(cloned);
            counterMapping.put(original, cloned);
        }

        if (getNestedLoopNames() != null) {
            for (Map.Entry<String, Object> pair : getNestedLoopNames().entrySet()) {
                final LabelledCounter original = (LabelledCounter) pair.getValue();
                final LabelledCounter cloned = counterMapping.get(original);
                if (cloned != null) {
                    targetLoopNames.put(pair.getKey(), cloned);
                }
            }
        }
    }
}
