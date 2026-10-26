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

import io.cucumber.core.options.CommandlineOptionsParser;
import io.cucumber.core.options.RuntimeOptions;
import io.cucumber.core.runtime.Runtime;
import io.cucumber.plugin.ConcurrentEventListener;
import io.cucumber.plugin.event.EventPublisher;
import io.cucumber.plugin.event.Result;
import io.cucumber.plugin.event.Status;
import io.cucumber.plugin.event.TestCaseFinished;
import org.apache.tinkerpop.gremlin.fluency.glue.TraversalTimeout;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParserException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Scores {@link Candidate} traversals by running each one through the feature test step definitions in place of its
 * scenario's own traversal. Each batch is written as a feature file of scenario copies under the working directory
 * and kept there, so any scored attempt can be run again exactly as it was scored. One scorer should run at a time
 * in a JVM because the traversal timeout is shared by the Cucumber hooks.
 */
public final class Scorer {
    private static final int MAX_DETAIL_LENGTH = 500;

    private final Path workDir;
    private final Duration timeout;
    private int batch = 0;

    /**
     * @param workDir where the feature files of scenario copies are written
     * @param timeout how long a candidate traversal may run before it is interrupted and scored as a timeout
     */
    public Scorer(final Path workDir, final Duration timeout) {
        this.workDir = Objects.requireNonNull(workDir, "workDir must not be null");
        this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
    }

    /**
     * Scores the candidates as one batch and returns their attempts in the same order.
     */
    public List<ScoredAttempt> score(final List<Candidate> candidates) throws IOException {
        final Set<String> attemptIds = new HashSet<>();
        for (Candidate candidate : candidates) {
            if (!attemptIds.add(candidate.getAttemptId()))
                throw new IllegalArgumentException("Duplicate attemptId: " + candidate.getAttemptId());
        }

        final Map<String, Result> results = new ConcurrentHashMap<>();
        if (candidates.stream().anyMatch(Candidate::hasTraversal)) {
            final Path features = workDir.resolve("features");
            Files.createDirectories(features);
            final Path feature = features.resolve(String.format("batch-%04d.feature", ++batch));
            Files.write(feature, FeatureWriter.write(candidates).getBytes(StandardCharsets.UTF_8));
            candidates.stream().filter(Candidate::hasTraversal).map(c -> c.getScenario().getGraph()).filter(Objects::nonNull).
                    distinct().forEach(FluencyWorld::load);
            run(feature, results);
        }

        final List<ScoredAttempt> attempts = new ArrayList<>();
        for (Candidate candidate : candidates) {
            if (!candidate.hasAnswer()) {
                attempts.add(new ScoredAttempt(candidate, Outcome.NO_ANSWER, null));
                continue;
            }
            if (!candidate.hasTraversal()) {
                attempts.add(new ScoredAttempt(candidate, Outcome.DECLINED, null));
                continue;
            }

            final Result result = results.get(candidate.getAttemptId());
            if (null == result)
                throw new IllegalStateException("No result for " + candidate.getAttemptId());
            attempts.add(toAttempt(candidate, result));
        }
        return attempts;
    }

    private void run(final Path feature, final Map<String, Result> results) {
        final RuntimeOptions options = new CommandlineOptionsParser(System.out).parse(
                "--glue", "org.apache.tinkerpop.gremlin.features",
                "--glue", TraversalTimeout.class.getPackage().getName(),
                "--object-factory", FluencyGuiceFactory.class.getName(),
                "--monochrome",
                feature.toString()).
                setNoSummary().
                setPublishQuiet(true).
                build();

        final ConcurrentEventListener collector = (EventPublisher publisher) ->
                publisher.registerHandlerFor(TestCaseFinished.class,
                        e -> results.put(e.getTestCase().getName(), e.getResult()));

        TraversalTimeout.setTimeout(timeout);
        Runtime.builder().
                withRuntimeOptions(options).
                withClassLoader(Scorer.class::getClassLoader).
                withAdditionalPlugins(collector).
                build().
                run();
    }

    private static ScoredAttempt toAttempt(final Candidate candidate, final Result result) {
        if (result.getStatus() == Status.PASSED)
            return new ScoredAttempt(candidate, Outcome.PASS, null);

        // anything other than a pass or a failure means the scenario copy itself is broken, which is a scorer bug
        // rather than something to blame on the candidate
        if (result.getStatus() != Status.FAILED)
            throw new IllegalStateException(String.format("Scenario copy %s finished as %s",
                    candidate.getAttemptId(), result.getStatus()), result.getError());

        final Throwable error = result.getError();
        return new ScoredAttempt(candidate, classify(error), detail(error));
    }

    /**
     * Classifies a scenario failure. The step definitions keep an error raised while parsing or iterating the
     * traversal and rethrow it, wrapped, from the step that checks the result, while a result that does not match
     * fails that step's assertion.
     */
    static Outcome classify(final Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof TraversalInterruptedException) return Outcome.TIMEOUT;
        }
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof GremlinParserException) return Outcome.PARSE_FAILURE;
        }
        return error instanceof AssertionError ? Outcome.WRONG_RESULT : Outcome.RUNTIME_ERROR;
    }

    private static String detail(final Throwable error) {
        if (null == error) return null;
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        final String message = root.getClass().getSimpleName() + (null == root.getMessage() ? "" : ": " + root.getMessage());
        return message.length() > MAX_DETAIL_LENGTH ? message.substring(0, MAX_DETAIL_LENGTH) + "..." : message;
    }
}
