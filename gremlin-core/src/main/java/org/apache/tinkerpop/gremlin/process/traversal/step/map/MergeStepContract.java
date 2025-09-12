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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Deleting;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.PropertiesHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.Map;
import java.util.Set;

public interface MergeStepContract<S, E, C> extends Writing<Event>, Deleting<Event>, TraversalOptionParent<Merge, S, C>, PropertiesHolder {

    Traversal.Admin<S, Map> getMergeTraversal();

    /**
     * Gets the merge map from this step. If the map was originally passed as a {@link GValue<Map>}, that is returned
     * directly. If it was originally passed as a {@link Map} or {@link ConstantTraversal<Map>}, then a {@link Map} is
     * returned. Otherwise, the MergeMap is returned in Traversal form.
     */
    default Object getMergeMapWithGValue() {
        Traversal mergeTraversal = getMergeTraversal();
        if (mergeTraversal instanceof ConstantTraversal) {
            return mergeTraversal.next();
        }
        return mergeTraversal;
    }

    Traversal.Admin<S, Map> getOnCreateTraversal();

    /**
     * Gets the onCreate map from this step. If the map was originally passed as a {@link GValue<Map>}, that is returned
     * directly. If it was originally passed as a {@link Map} or {@link ConstantTraversal<Map>}, then a {@link Map} is
     * returned. Otherwise, the onCreate is returned in Traversal form.
     */
    default Object getOnCreateMapWithGValue() {
        Traversal onCreateTraversal = getOnCreateTraversal();
        if (onCreateTraversal instanceof ConstantTraversal) {
            return onCreateTraversal.next();
        }
        return onCreateTraversal;
    }

    Traversal.Admin<S, Map<String, ?>> getOnMatchTraversal();

    /**
     * Gets the onMatch map from this step. If the map was originally passed as a {@link GValue<Map>}, that is returned
     * directly. If it was originally passed as a {@link Map} or {@link ConstantTraversal<Map>}, then a {@link Map} is
     * returned. Otherwise, the onMatch is returned in Traversal form.
     */
    default Object getOnMatchMapWithGValue() {
        Traversal onMatchTraversal = getOnMatchTraversal();
        if (onMatchTraversal instanceof ConstantTraversal) {
            return onMatchTraversal.next();
        }
        return onMatchTraversal;
    }

    boolean isStart();

    boolean isFirst();

    boolean isUsingPartitionStrategy();

    void reset();

    Set<TraverserRequirement> getRequirements();

    void setMerge(Traversal.Admin<?,Map<Object, Object>> mergeMap);

    void setOnCreate(Traversal.Admin<?,Map<Object, Object>> onCreateMap);

    void setOnMatch(Traversal.Admin<?,Map<Object, Object>> onMatchMap);
}
