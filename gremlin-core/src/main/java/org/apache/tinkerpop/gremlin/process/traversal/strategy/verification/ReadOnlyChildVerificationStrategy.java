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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.ReadOnlyTraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ReadOnlyChildValidator;

/**
 * Validates that child traversals in filter, lookup, and mutation steps do not contain mutating steps.
 * Serves as a safety net for programmatic traversal construction that bypasses the DSL's
 * construction-time validation.
 */
public final class ReadOnlyChildVerificationStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy>
        implements TraversalStrategy.VerificationStrategy {

    private static final ReadOnlyChildVerificationStrategy INSTANCE = new ReadOnlyChildVerificationStrategy();

    private ReadOnlyChildVerificationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof ReadOnlyTraversalParent) {
                for (final Traversal.Admin<?, ?> child : ((ReadOnlyTraversalParent) step).getLocalChildren()) {
                    try {
                        ReadOnlyChildValidator.validate(child);
                    } catch (final IllegalArgumentException e) {
                        throw new VerificationException(e.getMessage(), traversal);
                    }
                }
            }
        }
    }

    public static ReadOnlyChildVerificationStrategy instance() {
        return INSTANCE;
    }
}
