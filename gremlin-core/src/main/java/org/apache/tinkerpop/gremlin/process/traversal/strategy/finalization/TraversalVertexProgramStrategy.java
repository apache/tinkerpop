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

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalVertexProgramStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {

    private static final Set<Class<? extends FinalizationStrategy>> PRIORS = Collections.singleton(ProfileStrategy.class);
    private transient Function<Graph, GraphComputer> graphComputerFunction;

    private TraversalVertexProgramStrategy() {

    }

    public TraversalVertexProgramStrategy(final Function<Graph, GraphComputer> graphComputerFunction) {
        this.graphComputerFunction = graphComputerFunction;
    }


    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getParent() instanceof EmptyStep) {
            final Traversal.Admin newTraversal = new DefaultTraversal<>();
            TraversalHelper.removeToTraversal(traversal.getStartStep(), EmptyStep.instance(), newTraversal);
            traversal.addStep(new TraversalVertexProgramStep<>(traversal, newTraversal, this.graphComputerFunction.apply(traversal.getGraph().get())));
            TraversalVertexProgramStrategy.onlyGlobalChildren(newTraversal);
            traversal.addStep(new ComputerResultStep<>(traversal, true));
        }
    }

    private static void onlyGlobalChildren(final Traversal.Admin<?, ?> traversal) {
        for (final Step step : traversal.getSteps()) {
            if (step instanceof GraphComputing)
                ((GraphComputing) step).onGraphComputer();
            if (step instanceof TraversalParent) {
                ((TraversalParent) step).getGlobalChildren().forEach(TraversalVertexProgramStrategy::onlyGlobalChildren);
            }
        }
    }

    @Override
    public Set<Class<? extends FinalizationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static Optional<GraphComputer> getGraphComputer(final Graph graph, final TraversalStrategies strategies) {
        final Optional<TraversalStrategy<?>> optional = strategies.toList().stream().filter(strategy -> strategy instanceof TraversalVertexProgramStrategy).findAny();
        return optional.isPresent() ? Optional.of(((TraversalVertexProgramStrategy) optional.get()).graphComputerFunction.apply(graph)) : Optional.empty();
    }
}
