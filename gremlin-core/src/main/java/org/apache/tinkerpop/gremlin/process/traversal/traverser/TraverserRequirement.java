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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

/**
 * A {@link TraverserRequirement} is a list of requirements that a {@link Traversal} requires of a {@link Traverser}.
 * The less requirements, the simpler the traverser can be (both in terms of space and time constraints).
 * Every {@link Step} provides its specific requirements via {@link Step#getRequirements()}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum TraverserRequirement {

    /**
     * Indicates that the {@link Traverser} maintains a bulk count, which represents the multiplicity of
     * traversers being processed. This allows optimization by grouping multiple traversers with the same
     * state and treating them as a single entity to reduce computational overhead.
     */
    BULK,
    /**
     * Represents a {@link TraverserRequirement} indicating that the traverser must track labeled paths.
     * A labeled path is a collection of steps where specific steps have associated labels, enabling
     * retrieval of intermediate or final results based on these labels during a traversal.This requirement ensures
     * that the traverser has the capability to maintain and access  this labeled path information as it progresses
     * through the traversal.
     */
    LABELED_PATH,
    /**
     * Indicates that a {@link Traverser} supports handling nested loops within a traversal. This requirement is
     * relevant for traversals where steps can be executed within the context of multiple, potentially recursive
     * loop iterations, enabling complex traversal structures and control flow.
     */
    NESTED_LOOP,
    /**
     * Denotes that a traverser is required to carry an arbitrary object as its state.
     */
    OBJECT,
    /**
     * Represents a traverser requirement where each traverser instance is guaranteed to have a bulk of one. This
     * ensures that the traverser is processed individually and not in aggregated bulk.
     */
    ONE_BULK,
    /**
     * Represents the requirement for a traverser to maintain a path of the elements it has visited. This ensures that
     * the traverser can track its journey through the traversal graph to support path-based computations.
     */
    PATH,
    /**
     * Indicates that a traverser carries a "sack", which is a mutable structure used to hold aggregated or
     * intermediate results during the traversal process. This requirement allows steps to both read from and write to
     * the sack, enabling computations that span across multiple steps in a traversal.
     */
    SACK,
    /**
     * Indicates that a traverser is expected to interact with and leverage side-effects during the traversal process.
     * Side-effects are data that are collected, shared, or mutated as part of the traversal.
     */
    SIDE_EFFECTS,
    /**
     * Indicates that the traverser is required to support single loop iteration during the traversal.
     */
    SINGLE_LOOP

}
