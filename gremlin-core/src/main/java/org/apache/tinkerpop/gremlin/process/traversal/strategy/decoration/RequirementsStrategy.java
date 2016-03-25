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

package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.RequirementsStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RequirementsStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private final Set<TraverserRequirement> requirements = new HashSet<>();

    private RequirementsStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getParent() instanceof EmptyStep && !this.requirements.isEmpty())
            traversal.addStep(new RequirementsStep<>(traversal, this.requirements));
    }

    public static void addRequirements(final TraversalStrategies traversalStrategies, final TraverserRequirement... requirements) {
        RequirementsStrategy strategy = (RequirementsStrategy) traversalStrategies.toList().stream().filter(s -> s instanceof RequirementsStrategy).findAny().orElse(null);
        if (null == strategy) {
            strategy = new RequirementsStrategy();
            traversalStrategies.addStrategies(strategy);
        } else {
            final RequirementsStrategy cloneStrategy = new RequirementsStrategy();
            cloneStrategy.requirements.addAll(strategy.requirements);
            strategy = cloneStrategy;
            traversalStrategies.addStrategies(strategy);
        }
        Collections.addAll(strategy.requirements, requirements);
    }

    @Override
    public Set<Class<? extends DecorationStrategy>> applyPost() {
        return Collections.singleton(VertexProgramStrategy.class);
    }
}