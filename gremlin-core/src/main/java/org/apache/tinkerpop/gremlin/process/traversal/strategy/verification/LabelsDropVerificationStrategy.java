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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelsStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * Prevents {@code labels().drop()} patterns in traversals. Users should use
 * {@code dropLabel(label)} or {@code dropLabels()} instead.
 *
 * @since 4.0.0
 */
public final class LabelsDropVerificationStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy>
        implements TraversalStrategy.VerificationStrategy {

    private static final LabelsDropVerificationStrategy INSTANCE = new LabelsDropVerificationStrategy();

    private LabelsDropVerificationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        for (final DropStep<?> dropStep : TraversalHelper.getStepsOfClass(DropStep.class, traversal)) {
            Step<?, ?> current = dropStep.getPreviousStep();

            // Walk backward through filter steps (IsStep, HasStep, WhereStep, NotStep, etc.)
            while (current instanceof FilterStep) {
                current = current.getPreviousStep();
            }

            if (current instanceof LabelsStep) {
                throw new VerificationException(
                        "labels().drop() is not supported. Use dropLabel(label) or dropLabels() instead.",
                        traversal);
            }
        }
    }

    public static LabelsDropVerificationStrategy instance() {
        return INSTANCE;
    }
}
