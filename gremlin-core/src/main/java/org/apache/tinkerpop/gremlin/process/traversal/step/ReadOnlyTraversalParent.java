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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;

/**
 * Marker interface for {@link TraversalParent} steps whose child traversals must be read-only
 * (no mutating steps allowed). Examples include {@code has(key, traversal)},
 * {@code is(P.gt(traversal))}, {@code V(traversal)}, and {@code property(traversal)}.
 * <p>
 * The {@link org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyChildVerificationStrategy}
 * validates the local children of every step implementing this interface, rejecting child traversals that
 * contain mutating steps. New steps that accept such traversals should implement this interface so they are
 * validated automatically rather than relying on a hardcoded step list.
 *
 * @since 4.0.0
 */
public interface ReadOnlyTraversalParent extends TraversalParent {
}
