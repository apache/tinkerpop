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
import org.apache.tinkerpop.gremlin.process.traversal.step.Deleting;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MergeStepInterface<S, E, C> extends Writing<Event>, Deleting<Event>, TraversalOptionParent<Merge, S, C>, PropertyAdding {

    Traversal.Admin<S, Map> getMergeTraversal();

    Traversal.Admin<S, Map> getOnCreateTraversal();

    Traversal.Admin<S, Map<String, ?>> getOnMatchTraversal();

    boolean isStart();

    boolean isFirst();

    boolean isUsingPartitionStrategy();

    void reset();

    Set<TraverserRequirement> getRequirements();

    void setMerge(Traversal.Admin<?,Map<Object, Object>> mergeMap);

    void setOnCreate(Traversal.Admin<?,Map<Object, Object>> onCreateMap);

    void setOnMatch(Traversal.Admin<?,Map<Object, Object>> onMatchMap);
}
