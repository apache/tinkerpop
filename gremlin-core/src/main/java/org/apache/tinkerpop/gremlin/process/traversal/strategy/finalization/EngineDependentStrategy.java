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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine.Type;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.EngineDependent;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

/**
 * A {@link Step} can extend {@link EngineDependent}. If it does, that means that the steps internal logic is
 * modulated by whether the execution is via {@link Type#STANDARD} or {@link Type#COMPUTER}.
 * {@code EngineDependentStrategy} simply locates all engine dependent steps and provides the respective
 * {@link TraversalEngine}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EngineDependentStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy>
        implements TraversalStrategy.FinalizationStrategy {

    private static final EngineDependentStrategy INSTANCE = new EngineDependentStrategy();

    private EngineDependentStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        for (Step step : traversal.getSteps()) {
            if (step instanceof EngineDependent) {
                ((EngineDependent) step).onEngine(traversal.getEngine());
            }
        }
    }

    public static EngineDependentStrategy instance() {
        return INSTANCE;
    }
}
