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
package org.apache.tinkerpop.gremlin.structure.snapshot.graph;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clustering.connected.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.search.path.ShortestPathVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ProgramVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An in-memory {@link GraphComputer} specialized for the built-in algorithms that can execute directly over a
 * {@link CsrSnapshot}. Algorithm state is indexed by vertex ordinal and no edge-sized message maps are constructed.
 */
public final class CsrGraphComputer implements GraphComputer {

    static {
        TraversalStrategies.GlobalCache.registerStrategies(CsrGraphComputer.class,
                TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class).clone()
                        .removeStrategies(GraphFilterStrategy.class));
    }

    private static final String PAGE_RANK_PROPERTY = "gremlin.pageRankVertexProgram.property";
    private static final String PAGE_RANK_ALPHA = "gremlin.pageRankVertexProgram.alpha";
    private static final String PAGE_RANK_EPSILON = "gremlin.pageRankVertexProgram.epsilon";
    private static final String PAGE_RANK_ITERATIONS = "gremlin.pageRankVertexProgram.maxIterations";
    private static final String PAGE_RANK_EDGES = "gremlin.pageRankVertexProgram.edgeTraversal";
    private static final String PAGE_RANK_INITIAL = "gremlin.pageRankVertexProgram.initialRankTraversal";

    private static final String PEER_PROPERTY = "gremlin.peerPressureVertexProgram.property";
    private static final String PEER_ITERATIONS = "gremlin.peerPressureVertexProgram.maxIterations";
    private static final String PEER_DISTRIBUTE = "gremlin.peerPressureVertexProgram.distributeVote";
    private static final String PEER_EDGES = "gremlin.peerPressureVertexProgram.edgeTraversal";
    private static final String PEER_INITIAL = "gremlin.pageRankVertexProgram.initialVoteStrengthTraversal";

    private static final String CONNECTED_PROPERTY = "gremlin.connectedComponentVertexProgram.property";
    private static final String CONNECTED_EDGES = "gremlin.pageRankVertexProgram.edgeTraversal";

    private static final String SHORTEST_SOURCE = "gremlin.shortestPathVertexProgram.sourceVertexFilter";
    private static final String SHORTEST_TARGET = "gremlin.shortestPathVertexProgram.targetVertexFilter";
    private static final String SHORTEST_EDGES = "gremlin.shortestPathVertexProgram.edgeTraversal";
    private static final String SHORTEST_DISTANCE = "gremlin.shortestPathVertexProgram.distanceTraversal";
    private static final String SHORTEST_MAX_DISTANCE = "gremlin.shortestPathVertexProgram.maxDistance";
    private static final String SHORTEST_INCLUDE_EDGES = "gremlin.shortestPathVertexProgram.includeEdges";
    private static final String TRAVERSAL_VOTE_TO_HALT = "gremlin.traversalVertexProgram.voteToHalt";
    private static final String TRAVERSAL_MUTATED_MEMORY_KEYS =
            "gremlin.traversalVertexProgram.mutatedMemoryKeys";
    private static final String TRAVERSAL_COMPLETED_BARRIERS =
            "gremlin.traversalVertexProgram.completedBarriers";

    private final CsrGraph graph;
    private final ExecutorService computerService = Executors.newSingleThreadExecutor(r -> {
        final Thread thread = new Thread(r, CsrGraphComputer.class.getSimpleName() + "-boss");
        thread.setDaemon(true);
        return thread;
    });

    private ResultGraph resultGraph;
    private Persist persist;
    private VertexProgram<?> vertexProgram;
    private int workers = Runtime.getRuntime().availableProcessors();
    private boolean submitted;

    public CsrGraphComputer(final CsrGraph graph) {
        this.graph = Objects.requireNonNull(graph);
    }

    @Override
    public GraphComputer result(final ResultGraph resultGraph) {
        this.resultGraph = Objects.requireNonNull(resultGraph);
        return this;
    }

    @Override
    public GraphComputer persist(final Persist persist) {
        this.persist = Objects.requireNonNull(persist);
        return this;
    }

    @Override
    public GraphComputer program(final VertexProgram vertexProgram) {
        this.vertexProgram = Objects.requireNonNull(vertexProgram);
        return this;
    }

    @Override
    public GraphComputer mapReduce(final MapReduce mapReduce) {
        throw new UnsupportedOperationException("CsrGraphComputer does not support MapReduce");
    }

    @Override
    public GraphComputer workers(final int workers) {
        if (workers < 1) throw new IllegalArgumentException("The number of workers must be positive");
        this.workers = workers;
        return this;
    }

    @Override
    public GraphComputer vertices(final Traversal<Vertex, Vertex> vertexFilter) {
        throw GraphComputer.Exceptions.graphFilterNotSupported();
    }

    @Override
    public GraphComputer edges(final Traversal<Vertex, Edge> edgeFilter) {
        throw GraphComputer.Exceptions.graphFilterNotSupported();
    }

    @Override
    public GraphComputer vertexProperties(final Traversal<Vertex, ? extends Property<?>> vertexPropertyFilter) {
        throw GraphComputer.Exceptions.graphFilterNotSupported();
    }

    @Override
    public synchronized Future<ComputerResult> submit() {
        if (submitted) throw GraphComputer.Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        submitted = true;
        if (vertexProgram == null) throw GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers();
        if (!isSupported(vertexProgram)) {
            throw new UnsupportedOperationException("CsrGraphComputer supports PageRankVertexProgram, "
                    + "PeerPressureVertexProgram, ConnectedComponentVertexProgram, ShortestPathVertexProgram and "
                    + "read-only TraversalVertexProgram continuations; got "
                    + vertexProgram.getClass().getName());
        }

        GraphComputerHelper.validateProgramOnComputer(this, vertexProgram);
        if (!vertexProgram.getMapReducers().isEmpty())
            throw new UnsupportedOperationException("CsrGraphComputer does not support VertexProgram MapReduce jobs");
        if (vertexProgram instanceof TraversalVertexProgram) validateTraversalContinuation(vertexProgram);

        resultGraph = GraphComputerHelper.getResultGraphState(Optional.of(vertexProgram),
                Optional.ofNullable(resultGraph));
        persist = GraphComputerHelper.getPersistState(Optional.of(vertexProgram), Optional.ofNullable(persist));
        if (!features().supportsResultGraphPersistCombination(resultGraph, persist))
            throw GraphComputer.Exceptions.resultGraphPersistCombinationNotSupported(resultGraph, persist);
        if (workers > features().getMaxWorkers())
            throw GraphComputer.Exceptions.computerRequiresMoreWorkersThanSupported(workers,
                    features().getMaxWorkers());

        graph.retainSnapshot();
        final AtomicBoolean snapshotReleased = new AtomicBoolean();
        final FutureTask<ComputerResult> result = new FutureTask<ComputerResult>(this::executeWithSnapshot) {
            @Override
            protected void set(final ComputerResult value) {
                super.set(value);
                if (isCancelled() && value.graph() instanceof CsrGraph && value.graph() != graph)
                    ((CsrGraph) value.graph()).close();
            }

            @Override
            public void run() {
                try {
                    super.run();
                } finally {
                    if (snapshotReleased.compareAndSet(false, true)) graph.releaseSnapshot();
                }
            }
        };
        try {
            computerService.execute(result);
        } catch (final RuntimeException | Error e) {
            if (snapshotReleased.compareAndSet(false, true)) graph.releaseSnapshot();
            computerService.shutdownNow();
            throw e;
        }
        computerService.shutdown();
        return result;
    }

    private ComputerResult executeWithSnapshot() {
        final long start = System.currentTimeMillis();
        final MapMemory memory = new MapMemory();
        memory.addVertexProgramMemoryComputeKeys(vertexProgram);
        final Map<String, CsrComputePropertyColumn> output;
        int iterations = 0;
        try (Execution execution = new Execution(graph, workers)) {
            if (vertexProgram instanceof PageRankVertexProgram) {
                final AlgorithmResult result = execution.pageRank(state(vertexProgram));
                output = result.properties;
                iterations = result.iterations;
            } else if (vertexProgram instanceof PeerPressureVertexProgram) {
                final AlgorithmResult result = execution.peerPressure(state(vertexProgram));
                output = result.properties;
                iterations = result.iterations;
            } else if (vertexProgram instanceof ConnectedComponentVertexProgram) {
                final AlgorithmResult result = execution.connectedComponent(state(vertexProgram));
                output = result.properties;
                iterations = result.iterations;
            } else if (vertexProgram instanceof ShortestPathVertexProgram) {
                final ShortestPathResult result = execution.shortestPath(state(vertexProgram));
                output = result.properties;
                if (result.properties.isEmpty())
                    memory.set(ShortestPathVertexProgram.SHORTEST_PATHS, result.paths);
                iterations = result.iterations;
            } else {
                final TraversalResult result =
                        execution.traversal((TraversalVertexProgram) vertexProgram, state(vertexProgram),
                                persist != Persist.NOTHING);
                output = result.properties;
                memory.set(TraversalVertexProgram.HALTED_TRAVERSERS, result.haltedTraversers);
                iterations = 0;
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TraversalInterruptedException();
        } catch (final TraversalInterruptedException e) {
            throw e;
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new IllegalStateException("CsrGraphComputer execution failed", e);
        }

        checkInterrupted();
        memory.setIteration(iterations);
        memory.setRuntime(System.currentTimeMillis() - start);
        final org.apache.tinkerpop.gremlin.structure.Graph result;
        if (persist == Persist.NOTHING) {
            result = resultGraph == ResultGraph.ORIGINAL ? graph : EmptyGraph.instance();
        } else {
            final Map<String, CsrComputePropertyColumn> properties = new HashMap<>(graph.computeProperties());
            properties.putAll(output);
            result = graph.resultView(properties, persist == Persist.EDGES);
        }
        return new DefaultComputerResult(result, memory.asImmutable());
    }

    private static Configuration state(final VertexProgram<?> program) {
        final Configuration configuration = new BaseConfiguration();
        program.storeState(configuration);
        return configuration;
    }

    private static boolean isSupported(final VertexProgram<?> program) {
        return program instanceof PageRankVertexProgram || program instanceof PeerPressureVertexProgram
                || program instanceof ConnectedComponentVertexProgram || program instanceof ShortestPathVertexProgram
                || program instanceof TraversalVertexProgram;
    }

    private static void validateTraversalContinuation(final VertexProgram<?> program) {
        program.getMemoryComputeKeys().forEach(key -> {
            final String name = key.getKey();
            if (!name.equals(TraversalVertexProgram.HALTED_TRAVERSERS)
                    && !name.equals(TraversalVertexProgram.ACTIVE_TRAVERSERS)
                    && !name.equals(TRAVERSAL_VOTE_TO_HALT)
                    && !name.equals(TRAVERSAL_MUTATED_MEMORY_KEYS)
                    && !name.equals(TRAVERSAL_COMPLETED_BARRIERS)) {
                throw new UnsupportedOperationException("CsrGraphComputer does not support traversal memory side "
                        + "effects in TraversalVertexProgram: " + name);
            }
        });
    }

    @Override
    public Features features() {
        return new Features() {
            @Override
            public int getMaxWorkers() {
                return Math.max(1, Runtime.getRuntime().availableProcessors());
            }

            @Override
            public boolean supportsVertexAddition() {
                return false;
            }

            @Override
            public boolean supportsVertexRemoval() {
                return false;
            }

            @Override
            public boolean supportsVertexPropertyRemoval() {
                return false;
            }

            @Override
            public boolean supportsEdgeAddition() {
                return false;
            }

            @Override
            public boolean supportsEdgeRemoval() {
                return false;
            }

            @Override
            public boolean supportsEdgePropertyAddition() {
                return false;
            }

            @Override
            public boolean supportsEdgePropertyRemoval() {
                return false;
            }

            @Override
            public boolean supportsResultGraphPersistCombination(final ResultGraph resultGraph,
                                                                 final Persist persist) {
                return resultGraph == ResultGraph.NEW || persist == Persist.NOTHING;
            }

            @Override
            public boolean supportsGraphFilter() {
                return false;
            }
        };
    }

    @Override
    public String toString() {
        return StringFactory.graphComputerString(this);
    }

    private static final class AlgorithmResult {
        private final Map<String, CsrComputePropertyColumn> properties;
        private final int iterations;

        private AlgorithmResult(final String key, final CsrComputePropertyColumn column, final int iterations) {
            this.properties = Collections.singletonMap(key, column);
            this.iterations = iterations;
        }

        private AlgorithmResult(final Map<String, CsrComputePropertyColumn> properties, final int iterations) {
            this.properties = properties;
            this.iterations = iterations;
        }
    }

    private static final class ShortestPathResult {
        private final List<Path> paths;
        private final Map<String, CsrComputePropertyColumn> properties;
        private final int iterations;

        private ShortestPathResult(final List<Path> paths, final CsrComputePropertyColumn haltedByVertex,
                                   final int iterations) {
            this.paths = paths;
            this.properties = haltedByVertex == null ? Collections.emptyMap()
                    : Collections.singletonMap(TraversalVertexProgram.HALTED_TRAVERSERS, haltedByVertex);
            this.iterations = iterations;
        }
    }

    private static final class TraversalResult {
        private final TraverserSet<Object> haltedTraversers;
        private final Map<String, CsrComputePropertyColumn> properties;

        private TraversalResult(final TraverserSet<Object> haltedTraversers,
                                final CsrComputePropertyColumn haltedByVertex) {
            this.haltedTraversers = haltedTraversers;
            this.properties = haltedByVertex == null ? Collections.emptyMap()
                    : Collections.singletonMap(TraversalVertexProgram.HALTED_TRAVERSERS, haltedByVertex);
        }
    }

    @FunctionalInterface
    private interface RangeTask {
        void execute(int worker, int start, int end);
    }

    @FunctionalInterface
    private interface EdgeConsumer {
        void accept(int neighbor, int edge);
    }

    private static final class Execution implements AutoCloseable {
        private final CsrGraph graph;
        private final CsrSnapshot snapshot;
        private final int vertices;
        private final int workers;
        private final ExecutorService pool;

        private Execution(final CsrGraph graph, final int requestedWorkers) {
            this.graph = graph;
            this.snapshot = graph.snapshot();
            this.vertices = snapshot.vertexCount();
            this.workers = Math.max(1, Math.min(requestedWorkers, Math.max(1, vertices)));
            this.pool = Executors.newFixedThreadPool(workers, r -> {
                final Thread thread = new Thread(r, CsrGraphComputer.class.getSimpleName() + "-worker");
                thread.setDaemon(true);
                return thread;
            });
        }

        private AlgorithmResult pageRank(final Configuration configuration) throws InterruptedException {
            final String property = configuration.getString(PAGE_RANK_PROPERTY, PageRankVertexProgram.PAGE_RANK);
            final double alpha = configuration.getDouble(PAGE_RANK_ALPHA, 0.85d);
            final double epsilon = configuration.getDouble(PAGE_RANK_EPSILON, 0.00001d);
            final int maxIterations = configuration.getInt(PAGE_RANK_ITERATIONS, 20);
            if (maxIterations < 1) return new AlgorithmResult(Collections.emptyMap(), 0);
            final EdgeProjection edges = edgeProjection(configuration, PAGE_RANK_EDGES, Direction.OUT);
            final int[] filteredDegree;
            if (edges.hasLabelFilter()) {
                filteredDegree = new int[vertices];
                runRanges((worker, start, end) -> {
                    for (int vertex = start; vertex < end; vertex++) {
                        if ((vertex & 0x3fff) == 0) checkInterrupted();
                        filteredDegree[vertex] = edges.degree(vertex);
                    }
                });
            } else {
                filteredDegree = null;
            }

            double[] current = new double[vertices];
            double[] next = new double[vertices];
            for (int vertex = 0; vertex < vertices; vertex++) {
                final org.apache.tinkerpop.gremlin.structure.VertexProperty<Number> existing =
                        graph.vertexAt(vertex).property(property);
                if (existing.isPresent()) current[vertex] = existing.value().doubleValue();
            }
            final boolean hasInitial = configuration.containsKey(PAGE_RANK_INITIAL);
            if (hasInitial) {
                final PureTraversal<Vertex, ? extends Number> initial =
                        PureTraversal.loadState(configuration, PAGE_RANK_INITIAL, graph);
                for (int vertex = 0; vertex < vertices; vertex++) {
                    checkInterrupted();
                    next[vertex] = TraversalUtil.apply(graph.vertexAt(vertex), initial.get()).doubleValue();
                }
            }

            double teleportationEnergy = hasInitial ? 0.0d : 1.0d;
            int iteration = 0;
            for (int i = 1; i <= maxIterations; i++) {
                checkInterrupted();
                final double[] source = current;
                final double[] destination = next;
                final boolean initialIteration = i == 1;
                final double localTerminalEnergy = vertices == 0 ? 0.0d : teleportationEnergy / vertices;
                final double[] error = new double[workers];
                final double[] energy = new double[workers];
                runRanges((worker, start, end) -> {
                    double localError = 0.0d;
                    double localEnergy = 0.0d;
                    for (int vertex = start; vertex < end; vertex++) {
                        if ((vertex & 0x3fff) == 0) checkInterrupted();
                        final double rank = (initialIteration ? destination[vertex]
                                : edges.pageRankSum(vertex, source, filteredDegree, alpha)) + localTerminalEnergy;
                        destination[vertex] = rank;
                        localError += Math.abs(rank - source[vertex]);
                        localEnergy += (1.0d - alpha) * rank;
                        if (edges.degree(vertex, filteredDegree) == 0) localEnergy += alpha * rank;
                    }
                    error[worker] = localError;
                    energy[worker] = localEnergy;
                });
                double convergenceError = 0.0d;
                teleportationEnergy = 0.0d;
                for (int worker = 0; worker < workers; worker++) {
                    convergenceError += error[worker];
                    teleportationEnergy += energy[worker];
                }
                current = next;
                next = source;
                iteration = i;
                if (convergenceError < epsilon) break;
            }
            return new AlgorithmResult(property, new DoubleColumn(current), iteration);
        }

        private AlgorithmResult peerPressure(final Configuration configuration) throws InterruptedException {
            final String property = configuration.getString(PEER_PROPERTY, PeerPressureVertexProgram.CLUSTER);
            final int maxIterations = configuration.getInt(PEER_ITERATIONS, 30);
            final boolean distribute = configuration.getBoolean(PEER_DISTRIBUTE, false);
            final EdgeProjection edges = edgeProjection(configuration, PEER_EDGES, Direction.OUT);
            final double[] strength = new double[vertices];
            final PureTraversal<Vertex, ? extends Number> initial = configuration.containsKey(PEER_INITIAL)
                    ? PureTraversal.loadState(configuration, PEER_INITIAL, graph) : null;
            for (int vertex = 0; vertex < vertices; vertex++) {
                checkInterrupted();
                final double value = initial == null ? 1.0d
                        : TraversalUtil.apply(graph.vertexAt(vertex), initial.get()).doubleValue();
                strength[vertex] = distribute ? value / edges.degree(vertex) : value;
            }

            int[] current = new int[vertices];
            int[] next = new int[vertices];
            for (int vertex = 0; vertex < vertices; vertex++) current[vertex] = vertex;
            final VoteAccumulator[] accumulators = new VoteAccumulator[workers];
            for (int i = 0; i < workers; i++) accumulators[i] = new VoteAccumulator();

            int iteration = distribute ? 1 : 0;
            for (int voteIteration = 0; voteIteration < maxIterations; voteIteration++) {
                checkInterrupted();
                final int[] source = current;
                final int[] destination = next;
                final boolean[] unchanged = new boolean[workers];
                Arrays.fill(unchanged, true);
                runRanges((worker, start, end) -> {
                    final VoteAccumulator votes = accumulators[worker];
                    for (int vertex = start; vertex < end; vertex++) {
                        if ((vertex & 0x3fff) == 0) checkInterrupted();
                        votes.clear();
                        votes.add(source[vertex], strength[vertex]);
                        edges.forEachPredecessor(vertex,
                                (neighbor, edge) -> votes.add(source[neighbor], strength[neighbor]));
                        final int cluster = votes.largest(snapshot);
                        destination[vertex] = cluster < 0 ? vertex : cluster;
                        if (destination[vertex] != source[vertex]) unchanged[worker] = false;
                    }
                });
                current = next;
                next = source;
                iteration++;
                boolean halt = true;
                for (final boolean workerUnchanged : unchanged) halt &= workerUnchanged;
                if (halt) break;
            }
            return new AlgorithmResult(property, new OrdinalIdColumn(snapshot, current), iteration);
        }

        private AlgorithmResult connectedComponent(final Configuration configuration) throws InterruptedException {
            final String property = configuration.getString(CONNECTED_PROPERTY,
                    ConnectedComponentVertexProgram.COMPONENT);
            final EdgeProjection edges = edgeProjection(configuration, CONNECTED_EDGES, Direction.BOTH);
            int[] current = new int[vertices];
            int[] next = new int[vertices];
            for (int vertex = 0; vertex < vertices; vertex++) current[vertex] = vertex;

            int iteration = 0;
            while (true) {
                checkInterrupted();
                final int[] source = current;
                final int[] destination = next;
                final boolean[] unchanged = new boolean[workers];
                Arrays.fill(unchanged, true);
                runRanges((worker, start, end) -> {
                    for (int vertex = start; vertex < end; vertex++) {
                        if ((vertex & 0x3fff) == 0) checkInterrupted();
                        final int best = edges.minimumComponent(vertex, source);
                        destination[vertex] = best;
                        if (best != source[vertex]) unchanged[worker] = false;
                    }
                });
                current = next;
                next = source;
                iteration++;
                boolean halt = true;
                for (final boolean workerUnchanged : unchanged) halt &= workerUnchanged;
                if (halt) break;
            }
            final CsrComputePropertyColumn halted = configuredHaltedTraversers(configuration);
            if (halted == null)
                return new AlgorithmResult(property, new OrdinalStringColumn(snapshot, current), iteration);
            final Map<String, CsrComputePropertyColumn> properties = new HashMap<>();
            properties.put(property, new OrdinalStringColumn(snapshot, current));
            properties.put(TraversalVertexProgram.HALTED_TRAVERSERS, halted);
            return new AlgorithmResult(properties, iteration);
        }

        private ShortestPathResult shortestPath(final Configuration configuration) {
            final boolean standalone = !configuration.containsKey(VertexProgramStep.ROOT_TRAVERSAL);
            final PureTraversal<Vertex, ?> sourceFilter =
                    PureTraversal.loadState(configuration, SHORTEST_SOURCE, graph);
            final PureTraversal<Vertex, ?> targetFilter =
                    PureTraversal.loadState(configuration, SHORTEST_TARGET, graph);
            final PureTraversal<Edge, Number> distanceTraversal =
                    PureTraversal.loadState(configuration, SHORTEST_DISTANCE, graph);
            final boolean allSources = sourceFilter.equals(ShortestPathVertexProgram.DEFAULT_VERTEX_FILTER_TRAVERSAL);
            final boolean allTargets = targetFilter.equals(ShortestPathVertexProgram.DEFAULT_VERTEX_FILTER_TRAVERSAL);
            final boolean unitDistance =
                    distanceTraversal.equals(ShortestPathVertexProgram.DEFAULT_DISTANCE_TRAVERSAL);
            final EdgeProjection edges = edgeProjection(configuration, SHORTEST_EDGES, Direction.BOTH);
            final boolean includeEdges = configuration.getBoolean(SHORTEST_INCLUDE_EDGES, false);
            final Number configuredMaximum = configuration.containsKey(SHORTEST_MAX_DISTANCE)
                    ? (Number) configuration.getProperty(SHORTEST_MAX_DISTANCE) : null;
            final double maximum = configuredMaximum == null ? Double.POSITIVE_INFINITY
                    : configuredMaximum.doubleValue();
            if (Double.isNaN(maximum))
                throw new UnsupportedOperationException("CsrGraphComputer shortestPath does not support a NaN "
                        + "maximum distance");
            final java.util.Set<Path> result = new LinkedHashSet<>();
            final Object[] sourceTraversers = standalone ? null : shortestPathStarts(configuration);
            int searches = 0;

            for (int source = 0; source < vertices; source++) {
                checkInterrupted();
                if (standalone) {
                    if (!allSources && !TraversalUtil.test(graph.vertexAt(source), sourceFilter.get())) continue;
                } else if (sourceTraversers[source] == null) {
                    continue;
                }
                final int sourceVertex = source;
                searches++;
                final double[] distance = new double[vertices];
                Arrays.fill(distance, Double.POSITIVE_INFINITY);
                final Predecessors[] predecessors = new Predecessors[vertices];
                final PriorityQueue<QueueEntry> queue = new PriorityQueue<>();
                distance[sourceVertex] = 0.0d;
                queue.add(new QueueEntry(sourceVertex, 0.0d));

                while (!queue.isEmpty()) {
                    checkInterrupted();
                    final QueueEntry entry = queue.poll();
                    if (Double.compare(entry.distance, distance[entry.vertex]) != 0) continue;
                    edges.forEachFrom(entry.vertex, (neighbor, edge) -> {
                        final double edgeDistance = unitDistance ? 1.0d : edgeDistance(edge, distanceTraversal);
                        if (!Double.isFinite(edgeDistance) || edgeDistance < 0.0d)
                            throw new UnsupportedOperationException("CsrGraphComputer shortestPath does not support "
                                    + "negative or non-finite edge distances");
                        final double candidate = entry.distance + edgeDistance;
                        if (!Double.isFinite(candidate))
                            throw new UnsupportedOperationException("CsrGraphComputer shortestPath distance overflowed "
                                    + "the finite double range");
                        if (unitDistance && candidate > maximum) return;
                        final int comparison = Double.compare(candidate, distance[neighbor]);
                        if (comparison < 0) {
                            distance[neighbor] = candidate;
                            predecessors[neighbor] = new Predecessors(entry.vertex, edge);
                            queue.add(new QueueEntry(neighbor, candidate));
                        } else if (comparison == 0 && neighbor != sourceVertex) {
                            if (predecessors[neighbor] == null)
                                predecessors[neighbor] = new Predecessors(entry.vertex, edge);
                            else
                                predecessors[neighbor].add(entry.vertex, edge);
                        }
                    });
                }

                final boolean[] path = new boolean[vertices];
                for (int target = 0; target < vertices; target++) {
                    if (Double.isInfinite(distance[target]) || distance[target] > maximum
                            || !allTargets && !TraversalUtil.test(graph.vertexAt(target), targetFilter.get())) continue;
                    collectPaths(sourceVertex, target, predecessors, path, new IntPath(), includeEdges, result);
                }
            }
            if (standalone)
                return new ShortestPathResult(new ArrayList<>(result), null, searches);

            final Traversal.Admin<?, ?> root =
                    PureTraversal.loadState(configuration, VertexProgramStep.ROOT_TRAVERSAL, graph).getPure();
            final String stepId = configuration.getString(ProgramVertexProgramStep.STEP_ID);
            Step<?, ?> programStep = null;
            for (final Step<?, ?> step : root.getSteps()) {
                if (step.getId().equals(stepId)) {
                    programStep = step;
                    break;
                }
            }
            if (programStep == null)
                throw new IllegalStateException("Could not find shortestPath step " + stepId + " in its root traversal");

            final Object[] haltedByVertex = new Object[vertices];
            for (int source = 0; source < vertices; source++) {
                if (sourceTraversers[source] != null) haltedByVertex[source] = new TraverserSet<>();
            }
            for (final Path path : result) {
                final int source = snapshot.vertexOrdinal(((Vertex) path.get(0)).id());
                final TraverserSet<Object> starts = (TraverserSet<Object>) sourceTraversers[source];
                for (final Traverser.Admin<Object> start : starts) {
                    final Traverser.Admin<Object> traverser =
                            (Traverser.Admin<Object>) ((Traverser.Admin) start).split(path, (Step) programStep);
                    TraverserSet<Object> vertexTraversers = (TraverserSet<Object>) haltedByVertex[source];
                    if (vertexTraversers == null) {
                        vertexTraversers = new TraverserSet<>();
                        haltedByVertex[source] = vertexTraversers;
                    }
                    vertexTraversers.add(traverser);
                }
            }
            return new ShortestPathResult(Collections.emptyList(), new ObjectColumn(haltedByVertex), searches);
        }

        private Object[] shortestPathStarts(final Configuration configuration) {
            final Object[] starts = new Object[vertices];
            for (int vertex = 0; vertex < vertices; vertex++) {
                final org.apache.tinkerpop.gremlin.structure.VertexProperty<TraverserSet<Object>> property =
                        graph.vertexAt(vertex).property(TraversalVertexProgram.HALTED_TRAVERSERS);
                if (property.isPresent()) {
                    final TraverserSet<Object> copy = new TraverserSet<>();
                    for (final Traverser.Admin<Object> traverser : property.value()) {
                        if (!(traverser.get() instanceof Vertex))
                            throw new UnsupportedOperationException("CsrGraphComputer shortestPath continuation "
                                    + "requires halted traversers positioned at vertices");
                        if (snapshot.vertexOrdinal(((Vertex) traverser.get()).id()) != vertex) continue;
                        copy.add(traverser.split());
                    }
                    if (!copy.isEmpty()) starts[vertex] = copy;
                }
            }
            final TraverserSet<Object> configured = TraversalVertexProgram.loadHaltedTraversers(configuration);
            for (final Traverser.Admin<Object> traverser : configured) {
                if (!(traverser.get() instanceof Vertex))
                    throw new UnsupportedOperationException("CsrGraphComputer shortestPath continuation requires "
                            + "halted traversers positioned at vertices");
                final int ordinal = snapshot.vertexOrdinal(((Vertex) traverser.get()).id());
                if (ordinal < 0) continue;
                TraverserSet<Object> vertexStarts = (TraverserSet<Object>) starts[ordinal];
                if (vertexStarts == null) {
                    vertexStarts = new TraverserSet<>();
                    starts[ordinal] = vertexStarts;
                }
                vertexStarts.add(traverser.split());
            }
            return starts;
        }

        private TraversalResult traversal(final TraversalVertexProgram program,
                                          final Configuration configuration, final boolean retainByVertex) {
            final Traversal.Admin<Object, Object> traversal =
                    (Traversal.Admin<Object, Object>) program.getTraversal().getPure();
            traversal.setGraph(graph);
            final boolean returnHaltedTraversers = returnsHaltedTraversers(traversal);
            if (!returnHaltedTraversers && !retainByVertex)
                throw new UnsupportedOperationException("A chained TraversalVertexProgram requires persisted vertex "
                        + "properties for its halted traversers");
            final TraverserSet<Object> starts = new TraverserSet<>();
            for (final Traverser.Admin<Object> traverser :
                    TraversalVertexProgram.<Object>loadHaltedTraversers(configuration)) {
                starts.add(traverser.split());
            }
            if (!(traversal.getStartStep() instanceof GraphStep)) {
                for (int vertex = 0; vertex < vertices; vertex++) {
                    final org.apache.tinkerpop.gremlin.structure.VertexProperty<TraverserSet<Object>> property =
                            graph.vertexAt(vertex).property(TraversalVertexProgram.HALTED_TRAVERSERS);
                    if (property.isPresent()) {
                        for (final Traverser.Admin<Object> traverser : property.value()) {
                            starts.add(traverser.split());
                        }
                    }
                }
            }
            if (!starts.isEmpty()) {
                for (final Traverser.Admin<Object> traverser : starts) {
                    traverser.setStepId(traversal.getStartStep().getId());
                    traversal.addStart(traverser);
                }
            }

            final TraverserSet<Object> halted = new TraverserSet<>();
            final Object[] haltedByVertex = !returnHaltedTraversers || retainByVertex ? new Object[vertices] : null;
            final Step<?, Object> end = (Step<?, Object>) traversal.getEndStep();
            while (end.hasNext()) {
                checkInterrupted();
                final Traverser.Admin<Object> traverser = (Traverser.Admin<Object>) end.next().detach();
                if (returnHaltedTraversers) {
                    halted.add(traverser);
                } else {
                    if (!(traverser.get() instanceof Vertex))
                        throw new UnsupportedOperationException("A chained TraversalVertexProgram on "
                                + "CsrGraphComputer must halt at vertices");
                    final int ordinal = snapshot.vertexOrdinal(((Vertex) traverser.get()).id());
                    if (ordinal >= 0) {
                        TraverserSet<Object> vertexTraversers = (TraverserSet<Object>) haltedByVertex[ordinal];
                        if (vertexTraversers == null) {
                            vertexTraversers = new TraverserSet<>();
                            haltedByVertex[ordinal] = vertexTraversers;
                        }
                        vertexTraversers.add(traverser);
                    }
                }
            }
            return new TraversalResult(halted, haltedByVertex == null ? null : new ObjectColumn(haltedByVertex));
        }

        private boolean returnsHaltedTraversers(final Traversal.Admin<?, ?> traversal) {
            final Step<?, ?> next = traversal.getParent().asStep().getNextStep();
            return next instanceof ComputerResultStep || next instanceof EmptyStep
                    || next instanceof ProfileStep && next.getNextStep() instanceof ComputerResultStep;
        }

        private CsrComputePropertyColumn configuredHaltedTraversers(final Configuration configuration) {
            final TraverserSet<Object> configured = TraversalVertexProgram.loadHaltedTraversers(configuration);
            if (configured.isEmpty()) return null;
            final Object[] haltedByVertex = new Object[vertices];
            for (int vertex = 0; vertex < vertices; vertex++) {
                final org.apache.tinkerpop.gremlin.structure.VertexProperty<TraverserSet<Object>> property =
                        graph.vertexAt(vertex).property(TraversalVertexProgram.HALTED_TRAVERSERS);
                if (!property.isPresent()) continue;
                final TraverserSet<Object> copy = new TraverserSet<>();
                for (final Traverser.Admin<Object> traverser : property.value()) copy.add(traverser.split());
                haltedByVertex[vertex] = copy;
            }
            for (final Traverser.Admin<Object> traverser : configured) {
                if (!(traverser.get() instanceof Vertex))
                    throw new UnsupportedOperationException("CsrGraphComputer connectedComponent continuation "
                            + "requires halted traversers positioned at vertices");
                final int ordinal = snapshot.vertexOrdinal(((Vertex) traverser.get()).id());
                if (ordinal < 0) continue;
                TraverserSet<Object> vertexTraversers = (TraverserSet<Object>) haltedByVertex[ordinal];
                if (vertexTraversers == null) {
                    vertexTraversers = new TraverserSet<>();
                    haltedByVertex[ordinal] = vertexTraversers;
                }
                vertexTraversers.add(traverser.split());
            }
            return new ObjectColumn(haltedByVertex);
        }

        private double edgeDistance(final int edge, final PureTraversal<Edge, Number> distanceTraversal) {
            final Traversal.Admin<Edge, Number> traversal = distanceTraversal.getPure();
            traversal.addStart(traversal.getTraverserGenerator().generate(graph.edgeAt(edge),
                    traversal.getStartStep(), 1L));
            return traversal.tryNext().orElse(0).doubleValue();
        }

        private void collectPaths(final int source, final int vertex, final Predecessors[] predecessors,
                                  final boolean[] onPath, final IntPath reversePath, final boolean includeEdges,
                                  final java.util.Set<Path> output) {
            checkInterrupted();
            if (onPath[vertex]) return;
            onPath[vertex] = true;
            reversePath.addVertex(vertex);
            if (vertex == source) {
                Path path = ImmutablePath.make();
                for (int i = reversePath.size - 1; i >= 0; i--) {
                    path = path.extend(ReferenceFactory.detach(graph.vertexAt(reversePath.vertices[i])),
                            Collections.emptySet());
                    if (includeEdges && i > 0)
                        path = path.extend(ReferenceFactory.detach(graph.edgeAt(reversePath.edges[i - 1])),
                                Collections.emptySet());
                }
                output.add(path);
            } else {
                final Predecessors list = predecessors[vertex];
                if (list != null) {
                    for (int i = 0; i < list.size; i++) {
                        reversePath.setLastEdge(list.edges[i]);
                        collectPaths(source, list.vertices[i], predecessors, onPath, reversePath, includeEdges, output);
                    }
                }
            }
            reversePath.removeLast();
            onPath[vertex] = false;
        }

        private EdgeProjection edgeProjection(final Configuration configuration, final String key,
                                              final Direction defaultDirection) {
            if (!configuration.containsKey(key))
                return new EdgeProjection(defaultDirection, new int[0], graph.edgesVisible());
            final PureTraversal<Vertex, Edge> pure = PureTraversal.loadState(configuration, key, graph);
            final Traversal.Admin<Vertex, Edge> traversal = pure.getPure();
            final List<Step> steps = traversal.getSteps();
            if (steps.size() != 1 || !(steps.get(0) instanceof VertexStep)
                    || !((VertexStep<?>) steps.get(0)).returnsEdge()) {
                throw new UnsupportedOperationException("CsrGraphComputer requires " + key
                        + " to be a direct outE(), inE(), or bothE() traversal with optional edge labels");
            }
            final VertexStep<?> step = (VertexStep<?>) steps.get(0);
            final String[] labels = step.getEdgeLabels();
            final int[] codes = new int[labels.length];
            for (int i = 0; i < labels.length; i++) codes[i] = snapshot.edgeLabelCodeOf(labels[i]);
            return new EdgeProjection(step.getDirection(), codes, graph.edgesVisible());
        }

        private void runRanges(final RangeTask task) throws InterruptedException {
            final List<Future<?>> futures = new ArrayList<>(workers);
            for (int worker = 0; worker < workers; worker++) {
                final int index = worker;
                final int start = (int) ((long) vertices * worker / workers);
                final int end = (int) ((long) vertices * (worker + 1) / workers);
                futures.add(pool.submit(() -> task.execute(index, start, end)));
            }
            try {
                for (final Future<?> future : futures) future.get();
            } catch (final ExecutionException e) {
                for (final Future<?> future : futures) future.cancel(true);
                final Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) throw (RuntimeException) cause;
                if (cause instanceof Error) throw (Error) cause;
                throw new IllegalStateException(cause);
            } catch (final InterruptedException e) {
                for (final Future<?> future : futures) future.cancel(true);
                pool.shutdownNow();
                throw e;
            }
        }

        @Override
        public void close() {
            pool.shutdownNow();
            boolean interrupted = false;
            while (!pool.isTerminated()) {
                try {
                    pool.awaitTermination(1, TimeUnit.DAYS);
                } catch (final InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
        }

        private final class EdgeProjection {
            private final Direction direction;
            private final int[] labelCodes;
            private final boolean visible;

            private EdgeProjection(final Direction direction, final int[] labelCodes, final boolean visible) {
                this.direction = direction;
                this.labelCodes = labelCodes;
                this.visible = visible;
            }

            private int degree(final int vertex) {
                if (!visible) return 0;
                int count = 0;
                if (direction == Direction.OUT || direction == Direction.BOTH) {
                    for (long position = snapshot.outStart(vertex); position < snapshot.outEnd(vertex); position++) {
                        if (matches(snapshot.outEdge(position))) count++;
                    }
                }
                if (direction == Direction.IN || direction == Direction.BOTH) {
                    for (long position = snapshot.inStart(vertex); position < snapshot.inEnd(vertex); position++) {
                        if (matches(snapshot.inEdge(position))) count++;
                    }
                }
                return count;
            }

            private int degree(final int vertex, final int[] filteredDegree) {
                if (!visible) return 0;
                if (filteredDegree != null) return filteredDegree[vertex];
                int degree = 0;
                if (direction == Direction.OUT || direction == Direction.BOTH)
                    degree += snapshot.outDegree(vertex);
                if (direction == Direction.IN || direction == Direction.BOTH)
                    degree += snapshot.inDegree(vertex);
                return degree;
            }

            private boolean hasLabelFilter() {
                return labelCodes.length != 0;
            }

            private double pageRankSum(final int vertex, final double[] rank, final int[] filteredDegree,
                                       final double alpha) {
                if (!visible) return 0.0d;
                double sum = 0.0d;
                if (direction == Direction.OUT || direction == Direction.BOTH) {
                    for (long position = snapshot.inStart(vertex); position < snapshot.inEnd(vertex); position++) {
                        final int edge = snapshot.inEdge(position);
                        final int source = snapshot.inNeighbor(position);
                        final int degree = degree(source, filteredDegree);
                        if (matches(edge) && degree != 0) sum += alpha * rank[source] / degree;
                    }
                }
                if (direction == Direction.IN || direction == Direction.BOTH) {
                    for (long position = snapshot.outStart(vertex); position < snapshot.outEnd(vertex); position++) {
                        final int edge = snapshot.outEdge(position);
                        final int source = snapshot.outNeighbor(position);
                        final int degree = degree(source, filteredDegree);
                        if (matches(edge) && degree != 0) sum += alpha * rank[source] / degree;
                    }
                }
                return sum;
            }

            private int minimumComponent(final int vertex, final int[] components) {
                int best = components[vertex];
                if (!visible) return best;
                if (direction == Direction.OUT || direction == Direction.BOTH) {
                    for (long position = snapshot.inStart(vertex); position < snapshot.inEnd(vertex); position++) {
                        final int edge = snapshot.inEdge(position);
                        final int candidate = components[snapshot.inNeighbor(position)];
                        if (matches(edge) && compareIds(snapshot, candidate, best) < 0) best = candidate;
                    }
                }
                if (direction == Direction.IN || direction == Direction.BOTH) {
                    for (long position = snapshot.outStart(vertex); position < snapshot.outEnd(vertex); position++) {
                        final int edge = snapshot.outEdge(position);
                        final int candidate = components[snapshot.outNeighbor(position)];
                        if (matches(edge) && compareIds(snapshot, candidate, best) < 0) best = candidate;
                    }
                }
                return best;
            }

            private void forEachFrom(final int vertex, final EdgeConsumer consumer) {
                if (!visible) return;
                if (direction == Direction.OUT || direction == Direction.BOTH) {
                    for (long position = snapshot.outStart(vertex); position < snapshot.outEnd(vertex); position++) {
                        final int edge = snapshot.outEdge(position);
                        if (matches(edge)) consumer.accept(snapshot.outNeighbor(position), edge);
                    }
                }
                if (direction == Direction.IN || direction == Direction.BOTH) {
                    for (long position = snapshot.inStart(vertex); position < snapshot.inEnd(vertex); position++) {
                        final int edge = snapshot.inEdge(position);
                        if (matches(edge)) consumer.accept(snapshot.inNeighbor(position), edge);
                    }
                }
            }

            private void forEachPredecessor(final int vertex, final EdgeConsumer consumer) {
                if (!visible) return;
                if (direction == Direction.OUT || direction == Direction.BOTH) {
                    for (long position = snapshot.inStart(vertex); position < snapshot.inEnd(vertex); position++) {
                        final int edge = snapshot.inEdge(position);
                        if (matches(edge)) consumer.accept(snapshot.inNeighbor(position), edge);
                    }
                }
                if (direction == Direction.IN || direction == Direction.BOTH) {
                    for (long position = snapshot.outStart(vertex); position < snapshot.outEnd(vertex); position++) {
                        final int edge = snapshot.outEdge(position);
                        if (matches(edge)) consumer.accept(snapshot.outNeighbor(position), edge);
                    }
                }
            }

            private boolean matches(final int edge) {
                if (labelCodes.length == 0) return true;
                final int actual = snapshot.edgeLabelCode(edge);
                for (final int code : labelCodes) {
                    if (code == actual) return true;
                }
                return false;
            }
        }
    }

    private static int compareIds(final CsrSnapshot snapshot, final int first, final int second) {
        return snapshot.vertexId(first).toString().compareTo(snapshot.vertexId(second).toString());
    }

    private static void checkInterrupted() {
        if (Thread.currentThread().isInterrupted()) throw new TraversalInterruptedException();
    }

    private static final class DoubleColumn implements CsrComputePropertyColumn {
        private final double[] values;

        private DoubleColumn(final double[] values) {
            this.values = values;
        }

        @Override
        public boolean isPresent(final int ordinal) {
            return true;
        }

        @Override
        public Object value(final int ordinal) {
            return values[ordinal];
        }
    }

    private static final class OrdinalIdColumn implements CsrComputePropertyColumn {
        private final CsrSnapshot snapshot;
        private final int[] ordinals;

        private OrdinalIdColumn(final CsrSnapshot snapshot, final int[] ordinals) {
            this.snapshot = snapshot;
            this.ordinals = ordinals;
        }

        @Override
        public boolean isPresent(final int ordinal) {
            return true;
        }

        @Override
        public Object value(final int ordinal) {
            return snapshot.vertexId(ordinals[ordinal]);
        }
    }

    private static final class OrdinalStringColumn implements CsrComputePropertyColumn {
        private final CsrSnapshot snapshot;
        private final int[] ordinals;

        private OrdinalStringColumn(final CsrSnapshot snapshot, final int[] ordinals) {
            this.snapshot = snapshot;
            this.ordinals = ordinals;
        }

        @Override
        public boolean isPresent(final int ordinal) {
            return true;
        }

        @Override
        public Object value(final int ordinal) {
            return snapshot.vertexId(ordinals[ordinal]).toString();
        }
    }

    private static final class ObjectColumn implements CsrComputePropertyColumn {
        private final Object[] values;

        private ObjectColumn(final Object[] values) {
            this.values = values;
        }

        @Override
        public boolean isPresent(final int ordinal) {
            return values[ordinal] != null;
        }

        @Override
        public Object value(final int ordinal) {
            return values[ordinal];
        }
    }

    private static final class VoteAccumulator {
        private int[] keys = new int[16];
        private double[] values = new double[16];
        private int[] stamps = new int[16];
        private int generation = 1;
        private int size;

        private void clear() {
            size = 0;
            if (++generation == 0) {
                Arrays.fill(stamps, 0);
                generation = 1;
            }
        }

        private void add(final int key, final double value) {
            if ((size + 1) * 2 >= keys.length) grow();
            int slot = mix(key) & (keys.length - 1);
            while (stamps[slot] == generation) {
                if (keys[slot] == key) {
                    values[slot] += value;
                    return;
                }
                slot = (slot + 1) & (keys.length - 1);
            }
            stamps[slot] = generation;
            keys[slot] = key;
            values[slot] = value;
            size++;
        }

        private int largest(final CsrSnapshot snapshot) {
            int largest = -1;
            double largestValue = Double.MIN_VALUE;
            for (int slot = 0; slot < keys.length; slot++) {
                if (stamps[slot] != generation) continue;
                if (values[slot] > largestValue || values[slot] == largestValue
                        && (largest < 0 || compareIds(snapshot, keys[slot], largest) < 0)) {
                    largest = keys[slot];
                    largestValue = values[slot];
                }
            }
            return largest;
        }

        private void grow() {
            final int[] oldKeys = keys;
            final double[] oldValues = values;
            final int[] oldStamps = stamps;
            final int oldGeneration = generation;
            keys = new int[oldKeys.length << 1];
            values = new double[keys.length];
            stamps = new int[keys.length];
            generation = 1;
            size = 0;
            for (int slot = 0; slot < oldKeys.length; slot++) {
                if (oldStamps[slot] == oldGeneration) add(oldKeys[slot], oldValues[slot]);
            }
        }

        private static int mix(final int value) {
            int result = value;
            result ^= result >>> 16;
            result *= 0x7feb352d;
            result ^= result >>> 15;
            return result;
        }
    }

    private static final class QueueEntry implements Comparable<QueueEntry> {
        private final int vertex;
        private final double distance;

        private QueueEntry(final int vertex, final double distance) {
            this.vertex = vertex;
            this.distance = distance;
        }

        @Override
        public int compareTo(final QueueEntry other) {
            return Double.compare(distance, other.distance);
        }
    }

    private static final class Predecessors {
        private int[] vertices = new int[2];
        private int[] edges = new int[2];
        private int size;

        private Predecessors(final int vertex, final int edge) {
            add(vertex, edge);
        }

        private void add(final int vertex, final int edge) {
            if (size == vertices.length) {
                vertices = Arrays.copyOf(vertices, size << 1);
                edges = Arrays.copyOf(edges, size << 1);
            }
            vertices[size] = vertex;
            edges[size++] = edge;
        }
    }

    private static final class IntPath {
        private int[] vertices = new int[8];
        private int[] edges = new int[8];
        private int size;

        private void addVertex(final int vertex) {
            if (size == vertices.length) {
                vertices = Arrays.copyOf(vertices, size << 1);
                edges = Arrays.copyOf(edges, size << 1);
            }
            vertices[size++] = vertex;
        }

        private void setLastEdge(final int edge) {
            edges[size - 1] = edge;
        }

        private void removeLast() {
            size--;
        }
    }
}
