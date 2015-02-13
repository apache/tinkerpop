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
package org.apache.tinkerpop.gremlin.process.graph.traversal.strategy;

import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ComparatorHolderRemovalStrategy extends AbstractTraversalStrategy {

    private static final ComparatorHolderRemovalStrategy INSTANCE = new ComparatorHolderRemovalStrategy();

    private ComparatorHolderRemovalStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.isStandard())
            return;

        if (TraversalHelper.hasStepOfAssignableClass(ComparatorHolder.class, traversal)) {
            final Step endStep = traversal.getEndStep();
            TraversalHelper.getStepsOfAssignableClass(ComparatorHolder.class, traversal)
                    .stream()
                    .filter(step -> step != endStep)
                    .forEach(step -> traversal.removeStep((Step)step));
        }
    }

    public static ComparatorHolderRemovalStrategy instance() {
        return INSTANCE;
    }
}
