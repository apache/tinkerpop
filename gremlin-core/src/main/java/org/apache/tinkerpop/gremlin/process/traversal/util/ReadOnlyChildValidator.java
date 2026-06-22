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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;

import java.util.List;

/**
 * Validates that child traversals do not contain mutating steps. Child traversals used as
 * arguments to filter predicates ({@code has()}, {@code is()}, etc.), lookup steps
 * ({@code V(traversal)}, {@code E(traversal)}), and mutation steps ({@code property(traversal)})
 * must be read-only - their purpose is to compute values, not produce side effects.
 */
public final class ReadOnlyChildValidator {

    private ReadOnlyChildValidator() {
    }

    /**
     * Validates that a child traversal contains no {@link Mutating} steps at any nesting depth.
     * Throws {@link IllegalArgumentException} if one is found.
     */
    public static void validate(final Traversal.Admin<?, ?> child) {
        final List<Step> mutatingSteps = TraversalHelper.getStepsOfAssignableClassRecursively(Mutating.class, child);
        if (!mutatingSteps.isEmpty()) {
            final Step<?, ?> found = mutatingSteps.get(0);
            throw new IllegalArgumentException(String.format(
                    "Child traversal contains a mutating step '%s' in '%s'. " +
                    "Mutating steps are not allowed in child traversals.",
                    found.getClass().getSimpleName(),
                    child));
        }
    }
}
