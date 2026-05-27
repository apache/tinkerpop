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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;

/**
 * Validates child traversals to ensure they do not contain disallowed steps based on the context
 * in which they are used. This utility is shared between construction-time validation (in API methods)
 * and strategy-time validation ({@code ChildTraversalVerificationStrategy}).
 * <p>
 * Validation rules:
 * <ul>
 *   <li><b>FILTER / LOOKUP context:</b> No steps implementing {@link Mutating} are allowed at any nesting depth.</li>
 *   <li><b>MUTATION context:</b> Only {@link DropStep} is blocked; other mutating steps are permitted.</li>
 * </ul>
 * <p>
 * Recursion walks both {@link TraversalParent#getLocalChildren()} and
 * {@link TraversalParent#getGlobalChildren()} to detect mutations nested inside
 * {@code map()}, {@code union()}, {@code choose()}, {@code coalesce()}, or any other parent step.
 */
public final class ChildTraversalValidator {

    private ChildTraversalValidator() {
        // static utility
    }

    /**
     * Validates a child traversal used in filter context (has, is, all, any, none, choose predicate).
     * Throws {@link IllegalArgumentException} if any {@link Mutating} step is found.
     */
    public static void validateFilterContext(final Traversal.Admin<?, ?> child) {
        validateRecursive(child, ChildTraversalContext.FILTER);
    }

    /**
     * Validates a child traversal used in lookup context (V(traversal), E(traversal)).
     * Throws {@link IllegalArgumentException} if any {@link Mutating} step is found.
     */
    public static void validateLookupContext(final Traversal.Admin<?, ?> child) {
        validateRecursive(child, ChildTraversalContext.LOOKUP);
    }

    /**
     * Validates a child traversal used in mutation context (property(traversal)).
     * Throws {@link IllegalArgumentException} if a {@link DropStep} is found.
     */
    public static void validateMutationContext(final Traversal.Admin<?, ?> child) {
        validateRecursive(child, ChildTraversalContext.MUTATION);
    }

    /**
     * Recursively validates all steps in the child traversal and its nested children.
     */
    public static void validateRecursive(final Traversal.Admin<?, ?> child,
                                  final ChildTraversalContext context) {
        for (final Step<?, ?> step : child.getSteps()) {
            validateStep(step, context);
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> nested : ((TraversalParent) step).getLocalChildren()) {
                    validateRecursive(nested, context);
                }
                for (final Traversal.Admin<?, ?> nested : ((TraversalParent) step).getGlobalChildren()) {
                    validateRecursive(nested, context);
                }
            }
        }
    }

    private static void validateStep(final Step<?, ?> step, final ChildTraversalContext context) {
        switch (context) {
            case FILTER:
            case LOOKUP:
                if (step instanceof Mutating) {
                    throw new IllegalArgumentException(
                            "Child traversal in " + context.name().toLowerCase() + " context contains mutating step " +
                            step.getClass().getSimpleName() + ". Mutating steps are not allowed in this context.");
                }
                break;
            case MUTATION:
                if (step instanceof DropStep) {
                    throw new IllegalArgumentException(
                            "Child traversal in mutation context contains DropStep. " +
                            "Destructive steps are not allowed inside property(traversal).");
                }
                break;
            default:
                break;
        }
    }
}
