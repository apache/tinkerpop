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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Scoping {

    public static enum Variable {START, END}

    public static final Set<TraverserRequirement> TYPICAL_LOCAL_REQUIREMENTS = EnumSet.of(TraverserRequirement.OBJECT, TraverserRequirement.SIDE_EFFECTS);
    public static final Set<TraverserRequirement> TYPICAL_GLOBAL_REQUIREMENTS = EnumSet.of(TraverserRequirement.OBJECT, TraverserRequirement.LABELED_PATH, TraverserRequirement.SIDE_EFFECTS);
    public static final TraverserRequirement[] TYPICAL_LOCAL_REQUIREMENTS_ARRAY = new TraverserRequirement[]{TraverserRequirement.OBJECT, TraverserRequirement.SIDE_EFFECTS};
    public static final TraverserRequirement[] TYPICAL_GLOBAL_REQUIREMENTS_ARRAY = new TraverserRequirement[]{TraverserRequirement.OBJECT, TraverserRequirement.LABELED_PATH, TraverserRequirement.SIDE_EFFECTS};

    public default <S> S getScopeValue(final Pop pop, final String key, final Traverser.Admin<?> traverser) throws IllegalArgumentException {
        if (traverser.getSideEffects().exists(key))
            return traverser.getSideEffects().get(key);
        ///
        final Object object = traverser.get();
        if (object instanceof Map && ((Map<String, S>) object).containsKey(key))
            return ((Map<String, S>) object).get(key);
        ///
        final Path path = traverser.path();
        if (path.hasLabel(key))
            return path.get(pop, key);
        ///
        throw new IllegalArgumentException("Neither the sideEffects, map, nor path has a " + key + "-key: " + this);
    }

    public default <S> S getNullableScopeValue(final Pop pop, final String key, final Traverser.Admin<?> traverser) {
        if (traverser.getSideEffects().exists(key))
            return traverser.getSideEffects().get(key);
        ///
        final Object object = traverser.get();
        if (object instanceof Map && ((Map<String, S>) object).containsKey(key))
            return ((Map<String, S>) object).get(key);
        ///
        final Path path = traverser.path();
        if (path.hasLabel(key))
            return path.get(pop, key);
        ///
        return null;
    }

    public Set<String> getScopeKeys();
}
