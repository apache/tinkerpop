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
package org.apache.tinkerpop.gremlin.process.traversal.util;

/**
 * Classifies the context in which a child traversal is used, determining what validation rules apply.
 *
 * <ul>
 *   <li>{@link #FILTER} — child traversals inside {@code has()}, {@code is()}, {@code all()}, {@code any()},
 *       {@code none()}, or {@code choose()} predicates. No {@link org.apache.tinkerpop.gremlin.process.traversal.step.Mutating}
 *       steps are allowed.</li>
 *   <li>{@link #LOOKUP} — child traversals inside {@code V(traversal)} or {@code E(traversal)}. No
 *       {@link org.apache.tinkerpop.gremlin.process.traversal.step.Mutating} steps are allowed.</li>
 *   <li>{@link #MUTATION} — child traversals inside {@code property(traversal)}. Only
 *       {@link org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep} is blocked; other mutating
 *       steps are permitted because the traversal intentionally produces values from graph state.</li>
 *   <li>{@link #NONE} — not a child-traversal-bearing step, or a step whose children are not validated
 *       (e.g., {@code mergeV}, {@code mergeE}).</li>
 * </ul>
 */
public enum ChildTraversalContext {
    FILTER,
    LOOKUP,
    MUTATION,
    NONE
}
