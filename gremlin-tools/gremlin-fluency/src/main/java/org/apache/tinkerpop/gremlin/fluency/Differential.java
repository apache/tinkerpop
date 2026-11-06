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
package org.apache.tinkerpop.gremlin.fluency;

import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.features.StepDefinition;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.fluency.glue.TraversalTimeout;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Checks whether a candidate that passed its scenario only did so because of the scenario's data. For each seed it
 * builds the scenario's graph, runs any initializer and resolves the parameters, then perturbs the graph with
 * {@link Perturber} and runs the traversal, once with the reference on one copy and once with the candidate on
 * another. The two results are compared with each other, never with the scenario's expected result, and so is the
 * state each side leaves the graph in. The scenario's own steps are replayed through {@link StepDefinition}, so
 * parameters and side effects are handled exactly as in the feature tests.
 */
public final class Differential {
    private static final Pattern GRAPH = Pattern.compile("^the (\\w+) graph$");
    private static final Pattern PARAMETER = Pattern.compile("^using the parameter (\\w+) defined as \"(.*)\"$");
    private static final Pattern SIDE_EFFECT = Pattern.compile("^using the side effect (\\w+) defined as \"(.*)\"$");
    private static final int MAX_DETAIL_LENGTH = 500;

    private final List<Long> seeds;
    private final Duration timeout;
    private final Map<String, Observation> references = new HashMap<>();

    /**
     * @param seeds   the seeds of the perturbed graphs each attempt is checked on
     * @param timeout how long either traversal may run on a perturbed graph
     */
    public Differential(final List<Long> seeds, final Duration timeout) {
        this.seeds = Collections.unmodifiableList(new ArrayList<>(seeds));
        this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
    }

    /**
     * Checks a scored attempt that passed and returns it with what the differential pass found. Any other attempt is
     * returned as it is.
     */
    public ScoredAttempt check(final ScoredAttempt attempt) {
        if (attempt.getOutcome() != Outcome.PASS) return attempt;

        final IntentScenario scenario = attempt.getCandidate().getScenario();
        if (scenario.getTags().contains("@MultiLabel"))
            return attempt.withDifferential(DifferentialOutcome.NOT_CHECKED, "multi-label graphs are not perturbed");

        String notChecked = "no seeds";
        boolean agreed = false;
        for (long seed : seeds) {
            final Observation reference = references.computeIfAbsent(scenario.getName() + "#" + seed,
                    k -> observe(scenario, scenario.getTraversal(), seed, true));
            if (reference.error != null) {
                notChecked = String.format("seed %d: the reference failed: %s", seed, reference.error);
                continue;
            }

            final Observation candidate = observe(scenario, attempt.getCandidate().getTraversal(), seed, false);
            if (candidate.timedOut) {
                notChecked = String.format("seed %d: the candidate timed out", seed);
                continue;
            }
            if (candidate.error != null)
                return attempt.withDifferential(DifferentialOutcome.COINCIDENTAL,
                        detail(String.format("seed %d: the candidate raised %s", seed, candidate.error)));

            if (!candidate.stateDigest.equals(reference.stateDigest)) {
                final Observation full = observe(scenario, scenario.getTraversal(), seed, false);
                return attempt.withDifferential(DifferentialOutcome.STATE_MISMATCH,
                        detail(String.format("seed %d: %s", seed, difference(full.state, candidate.state))));
            }

            if (compareResults(scenario)) {
                if (!candidate.result.equals(reference.result))
                    return attempt.withDifferential(DifferentialOutcome.COINCIDENTAL,
                            detail(String.format("seed %d: %s", seed, difference(reference.result, candidate.result))));
                agreed = true;
            } else if (reference.mutated) {
                agreed = true;
            } else {
                notChecked = "the result is not compared and the reference changes nothing in the graph";
            }
        }

        return agreed ? attempt.withDifferential(DifferentialOutcome.AGREES, null) :
                attempt.withDifferential(DifferentialOutcome.NOT_CHECKED, detail(notChecked));
    }

    /**
     * Results are compared unless the scenario only asserts their size or allows any of several values, as those
     * scenarios tend to be nondeterministic.
     */
    static boolean compareResults(final IntentScenario scenario) {
        for (ScenarioStep step : scenario.getSteps()) {
            final String text = step.getText();
            if (text.startsWith("the result should be of") || text.startsWith("the result should have a count of"))
                return false;
            if (text.startsWith("the result should")) return true;
        }
        return false;
    }

    private Observation observe(final IntentScenario scenario, final String traversal, final long seed,
                                final boolean reference) {
        final String graphName = null == scenario.getGraph() ? "empty" : scenario.getGraph();
        final AbstractTinkerGraph graph = graphName.equals("empty") ?
                FluencyWorld.emptyGraph() : Perturber.copy(FluencyWorld.graph(graphName));
        final StepDefinition steps = new StepDefinition(new PerturbedWorld(graph));
        final Observation observation = new Observation();
        try {
            steps.beforeEachScenario(null);
            Canonical canonical = null;
            List<String> before = null;
            for (ScenarioStep step : scenario.getSteps()) {
                final String text = step.getText();
                final Matcher graphMatcher = GRAPH.matcher(text);
                final Matcher parameter = PARAMETER.matcher(text);
                final Matcher sideEffect = SIDE_EFFECT.matcher(text);
                if (graphMatcher.matches())
                    steps.givenTheXGraph(graphMatcher.group(1));
                else if (text.equals("the graph initializer of"))
                    steps.theGraphInitializerOf(step.getDocString());
                else if (parameter.matches())
                    steps.usingTheParameterXDefinedAsX(parameter.group(1), unescape(parameter.group(2)));
                else if (sideEffect.matches())
                    steps.usingTheSideEffectXDefinedAsX(sideEffect.group(1), unescape(sideEffect.group(2)));
                else if (text.equals("the traversal of")) {
                    // parameters are resolved above, before the additions, so a duplicated name cannot change them
                    Perturber.perturb(graph, seed);
                    canonical = new Canonical(graph);
                    if (reference) before = canonical.state(graph);
                    steps.theTraversalOf(traversal);
                } else if (text.equals("iterated to list"))
                    timed(steps::iteratedToList);
                else if (text.equals("iterated next"))
                    timed(steps::iteratedNext);
            }

            if (null == canonical) throw new IllegalStateException(scenario + " has no traversal");
            if (steps.getError() != null) {
                observation.error = describe(steps.getError());
                observation.timedOut = isTimeout(steps.getError());
                return observation;
            }

            observation.result = canonical.result(steps.getResult());
            observation.state = canonical.state(graph);
            observation.stateDigest = digest(observation.state);
            observation.mutated = reference && !observation.state.equals(before);

            // a cached reference keeps only what later attempts compare against
            if (reference) observation.state = null;
            return observation;
        } catch (Exception ex) {
            observation.error = describe(ex);
            observation.timedOut = isTimeout(ex);
            return observation;
        } finally {
            try {
                steps.afterEachScenario();
            } catch (Exception ignored) {
                // the graph is discarded anyway
            }
            graph.close();
        }
    }

    private void timed(final Runnable iterate) {
        TraversalTimeout.setTimeout(timeout);
        final TraversalTimeout watchdog = new TraversalTimeout();
        watchdog.arm();
        try {
            iterate.run();
        } finally {
            watchdog.disarm();
        }
    }

    private static boolean isTimeout(final Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof TraversalInterruptedException) return true;
        }
        return false;
    }

    private static String describe(final Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root.getClass().getSimpleName() + (null == root.getMessage() ? "" : ": " + root.getMessage());
    }

    /**
     * Cucumber's {@code {string}} parameter type removes the escaping of double quotes.
     */
    private static String unescape(final String value) {
        return value.replace("\\\"", "\"");
    }

    private static String difference(final List<String> reference, final List<String> candidate) {
        final Map<String, Integer> counts = new TreeMap<>();
        reference.forEach(r -> counts.merge(r, 1, Integer::sum));
        candidate.forEach(c -> counts.merge(c, -1, Integer::sum));
        final List<String> missing = new ArrayList<>();
        final List<String> extra = new ArrayList<>();
        counts.forEach((item, count) -> {
            for (int i = 0; i < Math.abs(count); i++) (count > 0 ? missing : extra).add(item);
        });
        return String.format("the candidate lacks %s and adds %s", sample(missing), sample(extra));
    }

    private static String digest(final List<String> entries) {
        try {
            final MessageDigest sha = MessageDigest.getInstance("SHA-256");
            for (String entry : entries) {
                sha.update(entry.getBytes(StandardCharsets.UTF_8));
                sha.update((byte) '\n');
            }
            return new BigInteger(1, sha.digest()).toString(16);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static String sample(final List<String> items) {
        if (items.isEmpty()) return "nothing";
        return items.size() == 1 ? items.get(0) : items.get(0) + " and " + (items.size() - 1) + " more";
    }

    private static String detail(final String detail) {
        return detail.length() > MAX_DETAIL_LENGTH ? detail.substring(0, MAX_DETAIL_LENGTH) + "..." : detail;
    }

    private static final class Observation {
        private String error;
        private boolean timedOut;
        private List<String> result;
        private List<String> state;
        private String stateDigest;
        private boolean mutated;
    }

    /**
     * Serves one prepared graph, whichever graph a scenario asks for.
     */
    private static final class PerturbedWorld implements World {
        private final AbstractTinkerGraph graph;

        private PerturbedWorld(final AbstractTinkerGraph graph) {
            this.graph = graph;
        }

        @Override
        public GraphTraversalSource getGraphTraversalSource(final GraphData graphData) {
            return graph.traversal();
        }
    }
}
