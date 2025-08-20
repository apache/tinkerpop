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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

public final class GValueReductionStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {

    private static final GValueReductionStrategy INSTANCE = new GValueReductionStrategy();

    private GValueReductionStrategy() {}

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.applyTraversalRecursively(t -> {
            // we get accused of concurrentmodification if we try a for(Iterable)
            final List<Step> steps = t.getSteps();
            for (int i = 0; i < steps.size(); i++) {
                if (steps.get(i) instanceof GValueHolder) {
                    ((GValueHolder) steps.get(i)).reduce();
                }
            }
        }, traversal);
    }

    public static GValueReductionStrategy instance() {
        return INSTANCE;
    }


}
