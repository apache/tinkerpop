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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalParent extends PopContaining, AutoCloseable {

    public default <S, E> List<Traversal.Admin<S, E>> getGlobalChildren() {
        return Collections.emptyList();
    }

    public default <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return Collections.emptyList();
    }

    public default void addLocalChild(final Traversal.Admin<?, ?> localChildTraversal) {
        throw new IllegalStateException("This traversal parent does not support the addition of local traversals: " + this.getClass().getCanonicalName());
    }

    public default void addGlobalChild(final Traversal.Admin<?, ?> globalChildTraversal) {
        throw new IllegalStateException("This traversal parent does not support the addition of global traversals: " + this.getClass().getCanonicalName());
    }

    public default void removeLocalChild(final Traversal.Admin<?, ?> localChildTraversal) {
        throw new IllegalStateException("This traversal parent does not support the removal of local traversals: " + this.getClass().getCanonicalName());
    }

    public default void removeGlobalChild(final Traversal.Admin<?, ?> globalChildTraversal) {
        throw new IllegalStateException("This traversal parent does not support the removal of global traversals: " + this.getClass().getCanonicalName());
    }

    public default void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        throw new IllegalStateException("This traversal parent does not support the replacement of local traversals: " + this.getClass().getCanonicalName());
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

    public default <S, E> Traversal.Admin<S, E> integrateChild(final Traversal.Admin<?, ?> childTraversal) {
        if (null == childTraversal)
            return null;
        childTraversal.setParent(this);
        childTraversal.getSideEffects().mergeInto(this.asStep().getTraversal().getSideEffects());
        childTraversal.setSideEffects(this.asStep().getTraversal().getSideEffects());
        return (Traversal.Admin<S, E>) childTraversal;
    }

    @Override
    default void close() throws Exception {
        for(final Traversal.Admin<?,?> traversal : this.getLocalChildren()) {
            traversal.close();
        }

        for(final Traversal.Admin<?,?> traversal: this.getGlobalChildren()) {
            traversal.close();
        }
    }

    @Override
    public default HashSet<PopInstruction> getPopInstructions() {
        final HashSet<PopInstruction> scopingInfos = new HashSet<>();
        for (final Traversal.Admin local: this.getLocalChildren()) {
            scopingInfos.addAll(TraversalHelper.getPopInstructions(local));
        }
        for (final Traversal.Admin global: this.getGlobalChildren()) {
            scopingInfos.addAll(TraversalHelper.getPopInstructions(global));
        }
        return scopingInfos;
    }
}
