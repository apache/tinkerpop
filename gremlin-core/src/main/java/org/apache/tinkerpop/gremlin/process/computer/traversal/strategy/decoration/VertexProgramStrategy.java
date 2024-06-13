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

package org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.VertexComputing;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ProgramVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class VertexProgramStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private static final VertexProgramStrategy INSTANCE = new VertexProgramStrategy(Computer.compute());

    private final Computer computer;

    private VertexProgramStrategy() {
        this(null);
    }

    public VertexProgramStrategy(final Computer computer) {
        this.computer = computer;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // VertexPrograms can only execute at the root level of a Traversal and should not be applied locally prior to RemoteStrategy
        if (!(traversal.isRoot())
                || traversal instanceof AbstractLambdaTraversal
                || traversal.getStrategies().getStrategy(RemoteStrategy.class).isPresent())
            return;

        // back propagate as()-labels off of vertex computing steps
        Step<?, ?> currentStep = traversal.getEndStep();
        final Set<String> currentLabels = new HashSet<>();
        while (!(currentStep instanceof EmptyStep)) {
            if (currentStep instanceof VertexComputing && !(currentStep instanceof ProgramVertexProgramStep)) {  // todo: is there a general solution?
                currentLabels.addAll(currentStep.getLabels());
                currentStep.clearLabels();
            } else {
                currentLabels.forEach(currentStep::addLabel);
                currentLabels.clear();
            }
            currentStep = currentStep.getPreviousStep();
        }

        // push GraphStep forward in the chain to reduce the number of TraversalVertexProgram compilations
        currentStep = traversal.getStartStep();
        while (!(currentStep instanceof EmptyStep)) {
            if (currentStep instanceof GraphStep && currentStep.getNextStep() instanceof VertexComputing) {
                int index = TraversalHelper.stepIndex(currentStep.getNextStep(), traversal);
                traversal.removeStep(currentStep);
                traversal.addStep(index, currentStep);
            } else
                currentStep = currentStep.getNextStep();
        }

        // wrap all non-VertexComputing steps into a TraversalVertexProgramStep
        currentStep = traversal.getStartStep();
        while (!(currentStep instanceof EmptyStep)) {
            final Traversal.Admin<?, ?> computerTraversal = new DefaultTraversal<>(traversal.getGremlinLang());
            final Step<?, ?> firstLegalOLAPStep = getFirstLegalOLAPStep(currentStep);
            final Step<?, ?> lastLegalOLAPStep = getLastLegalOLAPStep(currentStep);
            if (!(firstLegalOLAPStep instanceof EmptyStep)) {
                final int index = TraversalHelper.stepIndex(firstLegalOLAPStep, traversal);
                TraversalHelper.removeToTraversal(firstLegalOLAPStep, lastLegalOLAPStep.getNextStep(), (Traversal.Admin) computerTraversal);
                final TraversalVertexProgramStep traversalVertexProgramStep = new TraversalVertexProgramStep(traversal, computerTraversal);
                traversal.addStep(index, traversalVertexProgramStep);
            }
            currentStep = traversal.getStartStep();
            while (!(currentStep instanceof EmptyStep)) {
                if (!(currentStep instanceof VertexComputing))
                    break;
                currentStep = currentStep.getNextStep();
            }
        }

        // if the last vertex computing step is a TraversalVertexProgramStep convert to OLTP with ComputerResultStep
        TraversalHelper.getLastStepOfAssignableClass(VertexComputing.class, traversal).ifPresent(step -> {
            if (step instanceof TraversalVertexProgramStep) {
                final ComputerResultStep computerResultStep = new ComputerResultStep<>(traversal);
                ((TraversalVertexProgramStep) step).getGlobalChildren().get(0).getEndStep().getLabels().forEach(computerResultStep::addLabel);
                // labeling should happen in TraversalVertexProgram (perhaps MapReduce)
                TraversalHelper.insertAfterStep(computerResultStep, (Step) step, traversal);
            }
        });
        
        // if there is a dangling vertex computing step, add an identity traversal (solve this in the future with a specialized MapReduce)
        if (traversal.getEndStep() instanceof VertexComputing && !(traversal.getEndStep() instanceof TraversalVertexProgramStep)) {
            final TraversalVertexProgramStep traversalVertexProgramStep = new TraversalVertexProgramStep(traversal, __.identity().asAdmin());
            traversal.addStep(traversalVertexProgramStep);
            traversal.addStep(new ComputerResultStep<>(traversal));
        }
        // all vertex computing steps needs the graph computer function
        traversal.getSteps().stream().filter(step -> step instanceof VertexComputing).forEach(step -> ((VertexComputing) step).setComputer(this.computer));
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
        while (currentStep instanceof VertexComputing)
            currentStep = currentStep.getNextStep();
        while (!(currentStep instanceof EmptyStep)) {
            if (currentStep instanceof VertexComputing)
                return currentStep.getPreviousStep();
            currentStep = currentStep.getNextStep();
        }
        return EmptyStep.instance();
    }

    public static Optional<Computer> getComputer(final TraversalStrategies strategies) {
        final Optional<VertexProgramStrategy> optional = strategies.getStrategy(VertexProgramStrategy.class);
        return optional.isPresent() ? Optional.of(optional.get().computer) : Optional.empty();
    }

    public void addGraphComputerStrategies(final TraversalSource traversalSource) {
        Class<? extends GraphComputer> graphComputerClass;
        if (this.computer.getGraphComputerClass().equals(GraphComputer.class)) {
            try {
                graphComputerClass = this.computer.apply(traversalSource.getGraph()).getClass();
            } catch (final Exception e) {
                graphComputerClass = GraphComputer.class;
            }
        } else
            graphComputerClass = this.computer.getGraphComputerClass();
        final List<TraversalStrategy<?>> graphComputerStrategies = TraversalStrategies.GlobalCache.getStrategies(graphComputerClass).toList();
        traversalSource.getStrategies().addStrategies(graphComputerStrategies.toArray(new TraversalStrategy[graphComputerStrategies.size()]));
    }

    public static VertexProgramStrategy instance() {
        return INSTANCE;
    }

    ////////////////////////////////////////////////////////////

    public static final String GRAPH_COMPUTER = "graphComputer";
    public static final String WORKERS = "workers";
    public static final String PERSIST = "persist";
    public static final String RESULT = "result";
    public static final String VERTICES = "vertices";
    public static final String EDGES = "edges";

    @Override
    public Configuration getConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(GRAPH_COMPUTER, this.computer.getGraphComputerClass().getCanonicalName());
        if (-1 != this.computer.getWorkers())
            map.put(WORKERS, this.computer.getWorkers());
        if (null != this.computer.getPersist())
            map.put(PERSIST, this.computer.getPersist().name());
        if (null != this.computer.getResultGraph())
            map.put(RESULT, this.computer.getResultGraph().name());
        if (null != this.computer.getVertices())
            map.put(VERTICES, this.computer.getVertices());
        if (null != this.computer.getEdges())
            map.put(EDGES, this.computer.getEdges());
        map.putAll(this.computer.getConfiguration());
        return new MapConfiguration(map);
    }

    public static VertexProgramStrategy create(final Configuration configuration) {
        try {
            final VertexProgramStrategy.Builder builder = VertexProgramStrategy.build();
            for (final String key : (List<String>) IteratorUtils.asList(configuration.getKeys())) {
                if (key.equals(GRAPH_COMPUTER))
                    builder.graphComputer((Class) Class.forName(configuration.getString(key)));
                else if (key.equals(WORKERS))
                    builder.workers(configuration.getInt(key));
                else if (key.equals(PERSIST))
                    builder.persist(GraphComputer.Persist.valueOf(configuration.getString(key)));
                else if (key.equals(RESULT))
                    builder.result(GraphComputer.ResultGraph.valueOf(configuration.getString(key)));
                else if (key.equals(VERTICES))
                    builder.vertices((Traversal) configuration.getProperty(key));
                else if (key.equals(EDGES))
                    builder.edges((Traversal) configuration.getProperty(key));
                else
                    builder.configure(key, configuration.getProperty(key));
            }
            return builder.create();
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder {

        private Computer computer = Computer.compute();

        private Builder() {
        }

        public Builder computer(final Computer computer) {
            this.computer = computer;
            return this;
        }

        public Builder graphComputer(final Class<? extends GraphComputer> graphComputerClass) {
            this.computer = this.computer.graphComputer(graphComputerClass);
            return this;
        }

        public Builder configure(final String key, final Object value) {
            this.computer = this.computer.configure(key, value);
            return this;
        }

        public Builder configure(final Map<String, Object> configurations) {
            this.computer = this.computer.configure(configurations);
            return this;
        }

        public Builder workers(final int workers) {
            this.computer = this.computer.workers(workers);
            return this;
        }

        public Builder persist(final GraphComputer.Persist persist) {
            this.computer = this.computer.persist(persist);
            return this;
        }


        public Builder result(final GraphComputer.ResultGraph resultGraph) {
            this.computer = this.computer.result(resultGraph);
            return this;
        }

        public Builder vertices(final Traversal<Vertex, Vertex> vertices) {
            this.computer = this.computer.vertices(vertices);
            return this;
        }

        public Builder edges(final Traversal<Vertex, Edge> edges) {
            this.computer = this.computer.edges(edges);
            return this;
        }

        public VertexProgramStrategy create() {
            return new VertexProgramStrategy(this.computer);
        }
    }

}
