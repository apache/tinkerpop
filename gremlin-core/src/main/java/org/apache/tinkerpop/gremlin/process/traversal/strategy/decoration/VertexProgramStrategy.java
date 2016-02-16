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

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.VertexComputing;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRankVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class VertexProgramStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private transient Function<Graph, GraphComputer> graphComputerFunction;

    private VertexProgramStrategy() {

    }

    public VertexProgramStrategy(final Function<Graph, GraphComputer> graphComputerFunction) {
        this.graphComputerFunction = graphComputerFunction;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.getParent() instanceof EmptyStep))   // VertexPrograms can only execute at the root level of a Traversal
            return;

        traversal.addTraverserRequirement(TraverserRequirement.BULK); // all graph computations require bulk

        if (traversal.getStartStep() instanceof GraphStep && (traversal.getStartStep().getNextStep() instanceof PageRankVertexProgramStep)) {
            final GraphStep<?, ?> graphStep = (GraphStep) traversal.getStartStep();
            final PageRankVertexProgramStep pageRankVertexProgramStep = (PageRankVertexProgramStep) traversal.getStartStep().getNextStep();
            if (!graphStep.returnsVertex())
                throw new VerificationException("The GraphStep previous to PageRankVertexStep must emit vertices: " + graphStep, traversal);
            pageRankVertexProgramStep.setGraphComputerFunction(this.graphComputerFunction);
            graphStep.getLabels().forEach(pageRankVertexProgramStep::addLabel);
            traversal.removeStep(0);  // remove the graph step
            if (traversal.getSteps().size() == 1) // todo: in the future, this should just be a mapreduce job added to the PageRankVertexProgram step
                traversal.addStep(new IdentityStep<>(traversal));
        }
        if (null != this.graphComputerFunction) {   // if the function is null, then its been serialized and thus, already in a graph computer
            Traversal.Admin<?, ?> computerTraversal = new DefaultTraversal<>();
            Step<?, ?> firstLegalOLAPStep = getFirstLegalOLAPStep(traversal.getStartStep());
            Step<?, ?> lastLegalOLAPStep = getLastLegalOLAPStep(traversal.getStartStep());
            if (!(firstLegalOLAPStep instanceof EmptyStep)) {
                int index = TraversalHelper.stepIndex(firstLegalOLAPStep, traversal);
                TraversalHelper.removeToTraversal(firstLegalOLAPStep, lastLegalOLAPStep.getNextStep(), (Traversal.Admin) computerTraversal);
                final TraversalVertexProgramStep traversalVertexProgramStep = new TraversalVertexProgramStep(traversal, computerTraversal);
                traversalVertexProgramStep.setGraphComputerFunction(this.graphComputerFunction);
                final ComputerResultStep computerResultStep = new ComputerResultStep(traversal, true);
                if (!lastLegalOLAPStep.getLabels().isEmpty())
                    lastLegalOLAPStep.getLabels().forEach(computerResultStep::addLabel);
                traversal.addStep(index, traversalVertexProgramStep);
                traversal.addStep(index + 1, computerResultStep);
            }
        } else {  // this is a total hack to trick the difference between TraversalVertexProgram via GraphComputer and via TraversalSource. :|
            traversal.setParent(new TraversalVertexProgramStep(EmptyTraversal.instance(), EmptyTraversal.instance()));
            ComputerVerificationStrategy.instance().apply(traversal);
            traversal.setParent(EmptyStep.instance());
        }

    }

    private static Step<?, ?> getFirstLegalOLAPStep(Step<?, ?> currentStep) {
        while (!(currentStep instanceof EmptyStep)) {
            if (!(currentStep instanceof VertexComputing))
                return currentStep;
            currentStep = currentStep.getNextStep();
        }
        return EmptyStep.instance();
    }

    private static Step<?, ?> getLastLegalOLAPStep(Step<?, ?> currentStep) {
        while (!(currentStep instanceof EmptyStep)) {
            if (ComputerVerificationStrategy.isStepInstanceOfEndStep(currentStep))
                return currentStep;
            currentStep = currentStep.getNextStep();
        }
        return EmptyStep.instance();
    }

    public static Optional<GraphComputer> getGraphComputer(final Graph graph, final TraversalStrategies strategies) {
        final Optional<TraversalStrategy<?>> optional = strategies.toList().stream().filter(strategy -> strategy instanceof VertexProgramStrategy).findAny();
        return optional.isPresent() ? Optional.of(((VertexProgramStrategy) optional.get()).graphComputerFunction.apply(graph)) : Optional.empty();
    }
}
