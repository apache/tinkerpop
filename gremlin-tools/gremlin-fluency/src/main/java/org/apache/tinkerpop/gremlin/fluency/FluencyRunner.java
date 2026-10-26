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

import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.apache.tinkerpop.gremlin.util.TestSupport;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationFeature;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Runs every setup of a {@link Matrix} over the annotated scenarios and scores the candidates. A run writes to its
 * working directory a copy of the matrix, {@code run.json} describing the corpus and setups, {@code attempts.jsonl}
 * with one row per attempt, and the scenario copies that were scored, one directory per setup. Attempts that pass are
 * then checked by the {@link Differential} pass. A setup's attempts are made in as many lanes as the matrix's
 * parallelism, each lane with its own driver and, on the server, its own empty graph. A run ends by writing its
 * {@link Snapshot}. {@code run.json} is
 * written before the first attempt as well, so a run that stops early can still be summarized.
 * <p/>
 * Usage: {@code FluencyRunner <matrix.yaml> [--root <dir>]}. Each run's working directory is {@code runs/<run-id>}
 * under the root, which defaults to {@code ~/.gremlin-fluency} so that runs outlast a {@code mvn clean} and can be
 * compared later.
 */
public final class FluencyRunner {
    private static final ObjectMapper mapper = new ObjectMapper().disable(SerializationFeature.INDENT_OUTPUT);

    private FluencyRunner() {}

    public static void main(final String[] args) throws Exception {
        Path matrixFile = null;
        Path root = Paths.get(System.getProperty("user.home"), ".gremlin-fluency");
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--root") && i + 1 < args.length)
                root = Paths.get(args[++i]);
            else if (null == matrixFile)
                matrixFile = Paths.get(args[i]);
            else
                usage();
        }
        if (null == matrixFile) usage();

        final String runId = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        final Path workDir = root.resolve("runs").resolve(runId);

        Files.createDirectories(workDir);

        // the step definitions make scratch directories under build.dir. without it they work one out from where their
        // class was loaded, which from the gremlin-test jar is a "file:" path that lands in the current directory
        if (null == System.getProperty("build.dir"))
            System.setProperty("build.dir", workDir.toAbsolutePath().toString());

        Files.copy(matrixFile, workDir.resolve("matrix.yaml"), StandardCopyOption.REPLACE_EXISTING);
        final List<ScoredAttempt> attempts = run(runId, Matrix.load(matrixFile), Corpus.fromClasspath(), workDir);
        deleteScratch(workDir.resolve(TestSupport.TEST_DATA_RELATIVE_DIR));
        System.out.println(summary(attempts));
        System.out.println("Results and snapshot are in " + workDir.toAbsolutePath());

        // Gremlin Server can leave non-daemon threads behind after it closes, which would keep exec:java waiting
        System.exit(0);
    }

    /**
     * Removes what is left of the step definitions' scratch directories, which they empty after each scenario.
     */
    private static void deleteScratch(final Path scratch) throws IOException {
        if (!Files.exists(scratch)) return;
        try (Stream<Path> paths = Files.walk(scratch)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toArray(Path[]::new)) Files.delete(path);
        }
    }

    private static void usage() {
        System.err.println("Usage: FluencyRunner <matrix.yaml> [--root <dir>]");
        System.exit(1);
    }

    /**
     * Runs the matrix and returns every scored attempt, setup by setup.
     */
    public static List<ScoredAttempt> run(final String runId, final Matrix matrix, final Corpus corpus,
                                          final Path workDir) throws Exception {
        final List<IntentScenario> scenarios = corpus.getScenarios().stream().
                filter(s -> matrix.getGraphs().isEmpty() || matrix.getGraphs().contains(s.getGraph())).
                collect(Collectors.toList());

        final List<ScoredAttempt> all = new ArrayList<>();
        final Map<String, Object> run = new LinkedHashMap<>();
        run.put("runId", runId);
        run.put("version", Gremlin.version());
        run.put("git", git());
        run.put("promptHash", Prompt.templateHash());
        run.put("corpusHash", Hashes.of(scenarios.stream().map(s -> s.getName() + "=" + s.getHash()).
                sorted().collect(Collectors.joining("\n"))));
        run.put("attempts", matrix.getAttempts());
        run.put("parallelism", matrix.getParallelism());
        run.put("driverTimeout", matrix.getDriverTimeout().toString());
        run.put("traversalTimeout", matrix.getTraversalTimeout().toString());
        run.put("seeds", matrix.getSeeds());
        run.put("scenarios", scenarios.stream().map(FluencyRunner::scenarioRecord).collect(Collectors.toList()));
        run.put("exclusions", corpus.getExclusions());
        final Map<String, Map<String, Object>> setupRecords = new LinkedHashMap<>();
        for (Setup setup : matrix.getSetups()) {
            final Map<String, Object> setupRecord = new LinkedHashMap<>();
            setupRecord.put("name", setup.getName());
            setupRecord.put("model", setup.getModel());
            setupRecord.put("harness", setup.getHarness());
            setupRecord.put("context", setup.getContext());
            setupRecord.put("driver", null == setup.getBuiltin() ? setup.getCommand() : setup.getBuiltin());
            setupRecords.put(setup.getName(), setupRecord);
        }
        run.put("setups", new ArrayList<>(setupRecords.values()));

        final Differential differential = new Differential(matrix.getSeeds(), matrix.getTraversalTimeout());
        Files.createDirectories(workDir);
        mapper.writerWithDefaultPrettyPrinter().writeValue(workDir.resolve("run.json").toFile(), run);

        // one server for the whole run, serving every graph the selected scenarios use
        final boolean needsServer = matrix.getSetups().stream().anyMatch(Setup::usesServer);
        if (needsServer) System.err.println("Starting Gremlin Server for the setups that use one");
        final int lanes = matrix.getParallelism();
        try (FluencyServer server = needsServer ? FluencyServer.start(scenarios.stream().map(IntentScenario::getGraph).
                     distinct().collect(Collectors.toList()), lanes) : null;
             BufferedWriter rows = Files.newBufferedWriter(workDir.resolve("attempts.jsonl"), StandardCharsets.UTF_8)) {
            int attemptNumber = 0;
            for (Setup setup : matrix.getSetups()) {
                final List<Driver> drivers = new ArrayList<>();
                for (int lane = 0; lane < lanes; lane++) {
                    final int l = lane;
                    drivers.add(setup.createDriver(matrix.getDriverTimeout(),
                            null == server ? null : graphName -> server.endpoint(graphName, l)));
                }
                final List<Candidate> candidates = new ArrayList<>();
                final Map<String, Answer> answers = new LinkedHashMap<>();

                // attempts are numbered and collected in corpus order, whichever lane finishes first
                final BlockingQueue<Integer> freeLanes = new ArrayBlockingQueue<>(lanes);
                for (int lane = 0; lane < lanes; lane++) freeLanes.add(lane);
                final ExecutorService pool = Executors.newFixedThreadPool(lanes);
                final List<String> attemptIds = new ArrayList<>();
                final List<IntentScenario> attempted = new ArrayList<>();
                final List<Future<Answer>> pending = new ArrayList<>();
                try {
                    for (IntentScenario scenario : scenarios) {
                        final String prompt = Prompt.render(scenario);
                        for (int i = 1; i <= matrix.getAttempts(); i++) {
                            final int attempt = i;
                            final int number = ++attemptNumber;
                            attemptIds.add(scenario.getName() + "#" + setup.getName() + "#" + i);
                            attempted.add(scenario);
                            pending.add(pool.submit(() -> {
                                final int lane = freeLanes.take();
                                try {
                                    System.err.printf("%s: %s attempt %d of %d%n", setup.getName(), scenario.getName(),
                                            attempt, matrix.getAttempts());
                                    if (setup.usesServer()) server.prepare(scenario, lane);
                                    return drivers.get(lane).answer(scenario, prompt, number);
                                } finally {
                                    freeLanes.add(lane);
                                }
                            }));
                        }
                    }
                    for (int i = 0; i < pending.size(); i++) {
                        final Answer answer = await(pending.get(i));
                        answers.put(attemptIds.get(i), answer);
                        candidates.add(new Candidate(attempted.get(i), attemptIds.get(i), answer.getTraversal()));
                    }
                } finally {
                    pool.shutdownNow();
                }

                final List<ScoredAttempt> scored = new ArrayList<>();
                final Scorer scorer = new Scorer(workDir.resolve(setup.getName()), matrix.getTraversalTimeout());
                for (ScoredAttempt attempt : scorer.score(candidates)) {
                    if (attempt.getOutcome() == Outcome.PASS && !matrix.getSeeds().isEmpty())
                        System.err.printf("%s: checking %s on perturbed graphs%n", setup.getName(),
                                attempt.getCandidate().getAttemptId());
                    scored.add(matrix.getSeeds().isEmpty() ? attempt : differential.check(attempt));
                }
                for (ScoredAttempt attempt : scored) {
                    rows.write(mapper.writeValueAsString(row(setup, attempt, answers.get(attempt.getCandidate().getAttemptId()))));
                    rows.newLine();
                }
                rows.flush();
                all.addAll(scored);

                drivers.stream().filter(d -> d instanceof CommandDriver).map(d -> ((CommandDriver) d).getVersion()).
                        filter(v -> v != null).findFirst().
                        ifPresent(v -> setupRecords.get(setup.getName()).put("version", v));
            }
        }

        mapper.writerWithDefaultPrettyPrinter().writeValue(workDir.resolve("run.json").toFile(), run);
        Snapshot.write(workDir);
        return all;
    }

    private static Answer await(final Future<Answer> answer) throws Exception {
        try {
            return answer.get();
        } catch (ExecutionException ex) {
            throw ex.getCause() instanceof Exception ? (Exception) ex.getCause() : ex;
        }
    }

    private static Map<String, Object> scenarioRecord(final IntentScenario scenario) {
        final Map<String, Object> record = new LinkedHashMap<>();
        record.put("name", scenario.getName());
        record.put("feature", scenario.getFeaturePath());
        record.put("graph", scenario.getGraph());
        record.put("hash", scenario.getHash());
        record.put("intent", scenario.getIntent());
        record.put("reference", scenario.getTraversal());
        return record;
    }

    /**
     * The commit the run was made from and whether tracked files had uncommitted changes, which together identify the
     * runner and the feature files. Either is {@code null} when git cannot tell, such as outside a checkout.
     */
    static Map<String, Object> git() {
        final Map<String, Object> git = new LinkedHashMap<>();
        git.put("sha", command("git", "rev-parse", "HEAD"));
        final String status = command("git", "status", "--porcelain", "--untracked-files=no");
        git.put("dirty", null == status ? null : !status.isEmpty());
        return git;
    }

    private static String command(final String... command) {
        try {
            final Process process = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.DISCARD).start();
            final String out = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            return process.waitFor() == 0 ? out : null;
        } catch (IOException ex) {
            return null;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static Map<String, Object> row(final Setup setup, final ScoredAttempt attempt, final Answer answer) {
        final Candidate candidate = attempt.getCandidate();
        final Map<String, Object> row = new LinkedHashMap<>();
        row.put("setup", setup.getName());
        row.put("scenario", candidate.getScenario().getName());
        row.put("feature", candidate.getScenario().getFeaturePath());
        row.put("graph", candidate.getScenario().getGraph());
        row.put("attempt", candidate.getAttemptId());
        row.put("outcome", attempt.getOutcome().name());
        row.put("detail", attempt.getDetail());
        row.put("differential", null == attempt.getDifferential() ? null : attempt.getDifferential().name());
        row.put("differentialDetail", attempt.getDifferentialDetail());
        row.put("traversal", candidate.getTraversal());
        row.put("driverDetail", answer.getDetail());
        row.put("driverMillis", answer.getElapsedMillis());
        row.put("usage", answer.getUsage().isEmpty() ? null : answer.getUsage());
        row.put("tools", answer.getTools());
        return row;
    }

    /**
     * Counts of each outcome per setup, with the differential pass's findings for the passes. Comparisons between
     * setups belong to the snapshot, not this summary.
     */
    static String summary(final List<ScoredAttempt> attempts) {
        final Map<String, Map<Outcome, Integer>> counts = new LinkedHashMap<>();
        final Map<String, Map<DifferentialOutcome, Integer>> differentials = new LinkedHashMap<>();
        for (ScoredAttempt attempt : attempts) {
            final String setup = attempt.getCandidate().getAttemptId().split("#")[1];
            counts.computeIfAbsent(setup, k -> new EnumMap<>(Outcome.class)).merge(attempt.getOutcome(), 1, Integer::sum);
            final Map<DifferentialOutcome, Integer> found = differentials.computeIfAbsent(setup,
                    k -> new EnumMap<>(DifferentialOutcome.class));
            if (attempt.getDifferential() != null) found.merge(attempt.getDifferential(), 1, Integer::sum);
        }
        final StringBuilder summary = new StringBuilder();
        counts.forEach((setup, outcomes) -> {
            summary.append(setup).append(": ").append(outcomes);
            final Map<DifferentialOutcome, Integer> found = differentials.get(setup);
            if (!found.isEmpty()) {
                // a pass the differential pass could not check has nothing against it, so it counts as verified
                final int verified = found.getOrDefault(DifferentialOutcome.AGREES, 0) +
                        found.getOrDefault(DifferentialOutcome.NOT_CHECKED, 0);
                summary.append(" passes ").append(found).append(" verified=").append(verified);
            }
            summary.append('\n');
        });
        return summary.toString().trim();
    }
}
