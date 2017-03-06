/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Set;

/**
 * ScopingStrategy will analyze the traversal for step labels (e.g. as()) and provide {@link Scoping} steps that information.
 * This enables Scoping steps to avoid  having to generate step label data at {@link Step#getRequirements()} and thus,
 * may significantly reduce compilation times.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ScopingStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {

    private static final ScopingStrategy INSTANCE = new ScopingStrategy();


    private ScopingStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // only operate on the root traversal and only if it contains scoping steps
        if (!(traversal.getParent() instanceof EmptyStep) ||
                !TraversalHelper.hasStepOfAssignableClassRecursively(Scoping.class, traversal))
            return;

        // get the labels associated with the traveral
        final Set<String> labels = TraversalHelper.getLabels(traversal);
        // tell all scoping steps what those labels are
        for (final Scoping scoping : TraversalHelper.getStepsOfAssignableClassRecursively(Scoping.class, traversal)) {
            scoping.setPathLabels(labels);
        }
    }

    public static final ScopingStrategy instance() {
        return INSTANCE;
    }
}
