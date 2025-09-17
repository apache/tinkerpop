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

import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface HasContainerHolder<S, E> extends GValueHolder<S, E> {

    public List<HasContainer> getHasContainers();

    public void addHasContainer(final HasContainer hasContainer);

    public default void removeHasContainer(final HasContainer hasContainer) {
        throw new UnsupportedOperationException("The holder does not support container removal: " + this.getClass().getCanonicalName());
    }

    public default Collection<P<?>> getPredicates() {
        Collection<P<?>> predicates = getPredicatesGValueSafe();
        for (P<?> predicate : predicates) {
            if (predicate.isParameterized()) {
                getTraversal().getGValueManager().pinGValues(predicate.getGValues());
            }
        }
        return predicates;
    }

    public default Collection<P<?>> getPredicatesGValueSafe() {
        return getHasContainers().stream().map(p -> p.getPredicate()).collect(Collectors.toList());
    }

    public default HasContainerHolder<S, E> asConcreteStep() {
        return this;
    }

    public default boolean isParameterized() {
        return getPredicatesGValueSafe().stream().anyMatch(P::isParameterized);
    }

    public default void updateVariable(final String name, final Object value) {
        getPredicatesGValueSafe().forEach((p) -> p.updateVariable(name, value));
    }

    public default Collection<GValue<?>> getGValues() {
        Set<GValue<?>> allGValues = new HashSet<>();
        for (final P<?> p : getPredicatesGValueSafe()) {
            allGValues.addAll(p.getGValues());
        }
        return allGValues;
    }
}
