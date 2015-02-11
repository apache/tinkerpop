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
package com.apache.tinkerpop.gremlin.process.traversal.step;

import com.apache.tinkerpop.gremlin.process.Step;
import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.process.TraversalStrategies;
import com.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalParent {

    public enum Operation {
        SET_PARENT,
        SET_SIDE_EFFECTS,
        MERGE_IN_SIDE_EFFECTS,
    }

    public static final Operation[] TYPICAL_GLOBAL_OPERATIONS = {Operation.SET_PARENT, Operation.MERGE_IN_SIDE_EFFECTS, Operation.SET_SIDE_EFFECTS};
    public static final Operation[] TYPICAL_LOCAL_OPERATIONS = {Operation.SET_PARENT};

    public default <S, E> List<Traversal.Admin<S, E>> getGlobalChildren() {
        return Collections.emptyList();
    }

    public default <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return Collections.emptyList();
    }

    public default void addLocalChild(final Traversal.Admin<?, ?> localChildTraversal) {
        throw new IllegalStateException("This traversal holder does not support the addition of local traversals: " + this.getClass().getCanonicalName());
    }

    public default void addGlobalChild(final Traversal.Admin<?, ?> globalChildTraversal) {
        throw new IllegalStateException("This traversal holder does not support the addition of global traversals: " + this.getClass().getCanonicalName());
    }

    public default void setChildStrategies(final TraversalStrategies strategies) {
        this.getGlobalChildren().forEach(traversal -> traversal.setStrategies(strategies));
        this.getLocalChildren().forEach(traversal -> traversal.setStrategies(strategies));
    }

    public default Set<TraverserRequirement> getSelfAndChildRequirements(final TraverserRequirement... selfRequirements) {
        final Set<TraverserRequirement> requirements = EnumSet.noneOf(TraverserRequirement.class);
        Collections.addAll(requirements, selfRequirements);
        for (final Traversal.Admin<?, ?> local : this.getLocalChildren()) {
            requirements.addAll(local.getTraverserRequirements());
        }
        for (final Traversal.Admin<?, ?> global : this.getGlobalChildren()) {
            requirements.addAll(global.getTraverserRequirements());
        }
        return requirements;
    }

    public default Step<?, ?> asStep() {
        return (Step<?, ?>) this;
    }

    public default <S, E> Traversal.Admin<S, E> integrateChild(final Traversal.Admin<?, ?> childTraversal, final Operation... operations) {
        for (final Operation operation : operations) {
            switch (operation) {
                case SET_PARENT:
                    childTraversal.setParent(this);
                    break;
                case MERGE_IN_SIDE_EFFECTS:
                    childTraversal.getSideEffects().mergeInto(this.asStep().getTraversal().getSideEffects());
                    break;
                case SET_SIDE_EFFECTS:
                    childTraversal.setSideEffects(this.asStep().getTraversal().getSideEffects());
                    break;
            }
        }
        return (Traversal.Admin<S, E>) childTraversal;
    }
}
