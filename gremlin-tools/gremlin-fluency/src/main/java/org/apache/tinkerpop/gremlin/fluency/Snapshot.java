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

import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Summarizes one run from its {@code run.json} and {@code attempts.jsonl} into {@code snapshot.json} in the same
 * directory. A snapshot holds what identifies the run, each setup's raw and verified pass rates overall and by graph
 * and feature file, its outcome counts, reported usage and tool calls, each context's difference from {@code none} within a model
 * and harness with a bootstrap interval, and for every scenario each setup's pass rates and the distinct traversals it
 * wrote, with the tool calls behind them. It reads nothing else, so it can be rebuilt for any past run, or a run that stopped early, by running this
 * class with the run's directory.
 * <p/>
 * A pass is verified unless the differential pass found it {@code COINCIDENTAL} or {@code STATE_MISMATCH}.
 */
public final class Snapshot {
    /**
     * Resamples drawn for each bootstrap interval. The seed is fixed so that rebuilding a snapshot gives the same
     * intervals.
     */
    static final int RESAMPLES = 2000;
    static final long SEED = 1;

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<Map<String, Object>>() {};
    private static final String NONE = "none";

    private Snapshot() {}

    public static void main(final String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: Snapshot <run-directory>");
            System.exit(1);
        }
        final Path runDir = Paths.get(args[0]);
        write(runDir);
        System.out.println("Wrote snapshot.json and report.html to " + runDir.toAbsolutePath());
    }

    /**
     * Builds the snapshot of the run in the directory and writes it there as {@code snapshot.json}, with its
     * {@link Report}.
     */
    public static Map<String, Object> write(final Path runDir) throws IOException {
        final Map<String, Object> snapshot = build(runDir);
        mapper.writerWithDefaultPrettyPrinter().writeValue(runDir.resolve("snapshot.json").toFile(), snapshot);
        Report.write(snapshot, runDir);
        return snapshot;
    }

    static Map<String, Object> build(final Path runDir) throws IOException {
        final Map<String, Object> run = mapper.readValue(runDir.resolve("run.json").toFile(), MAP);
        final List<Map<String, Object>> rows = new ArrayList<>();
        final Path attempts = runDir.resolve("attempts.jsonl");
        if (Files.exists(attempts)) {
            for (String line : Files.readAllLines(attempts, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) rows.add(mapper.readValue(line, MAP));
            }
        }

        // rows by setup and then scenario, in the order they were written
        final Map<String, Map<String, List<Map<String, Object>>>> bySetup = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            bySetup.computeIfAbsent(text(row, "setup"), k -> new LinkedHashMap<>()).
                    computeIfAbsent(text(row, "scenario"), k -> new ArrayList<>()).add(row);
        }

        final Map<String, Object> snapshot = new LinkedHashMap<>();
        for (String key : Arrays.asList("runId", "version", "git", "promptHash", "corpusHash", "attempts", "seeds"))
            snapshot.put(key, run.get(key));

        final List<Map<String, Object>> setups = records(run, "setups");
        final List<Map<String, Object>> setupSummaries = new ArrayList<>();
        for (Map<String, Object> setup : setups) {
            final List<Map<String, Object>> setupRows = flatten(bySetup.get(text(setup, "name")));
            final Map<String, Object> summary = new LinkedHashMap<>(setup);
            summary.putAll(rates(setupRows));
            summary.put("outcomes", counts(setupRows, "outcome", r -> true));
            summary.put("differential", counts(setupRows, "differential", Snapshot::isPass));
            summary.put("usage", usage(setupRows));
            summary.put("tools", tools(setupRows));
            summary.put("byGraph", slices(setupRows, "graph"));
            summary.put("byFeature", slices(setupRows, "feature"));
            setupSummaries.add(summary);
        }
        snapshot.put("setups", setupSummaries);
        snapshot.put("comparisons", comparisons(setups, bySetup));

        final List<Map<String, Object>> scenarioSummaries = new ArrayList<>();
        for (Map<String, Object> scenario : records(run, "scenarios")) {
            final Map<String, Object> summary = new LinkedHashMap<>(scenario);
            final Map<String, Object> results = new LinkedHashMap<>();
            for (Map<String, Object> setup : setups) {
                final Map<String, List<Map<String, Object>>> scenarioRows = bySetup.get(text(setup, "name"));
                if (null == scenarioRows || !scenarioRows.containsKey(text(scenario, "name"))) continue;
                final List<Map<String, Object>> attemptRows = scenarioRows.get(text(scenario, "name"));
                final Map<String, Object> result = new LinkedHashMap<>(rates(attemptRows));
                result.put("traversals", traversals(attemptRows));
                results.put(text(setup, "name"), result);
            }
            summary.put("setups", results);
            scenarioSummaries.add(summary);
        }
        snapshot.put("scenarios", scenarioSummaries);
        return snapshot;
    }

    static boolean isPass(final Map<String, Object> row) {
        return Outcome.PASS.name().equals(row.get("outcome"));
    }

    static boolean isVerified(final Map<String, Object> row) {
        return isPass(row) && !DifferentialOutcome.COINCIDENTAL.name().equals(row.get("differential")) &&
                !DifferentialOutcome.STATE_MISMATCH.name().equals(row.get("differential"));
    }

    private static Map<String, Object> rates(final List<Map<String, Object>> rows) {
        final long passes = rows.stream().filter(Snapshot::isPass).count();
        final long verified = rows.stream().filter(Snapshot::isVerified).count();
        final Map<String, Object> rates = new LinkedHashMap<>();
        rates.put("attempts", rows.size());
        rates.put("passes", passes);
        rates.put("verified", verified);
        rates.put("passRate", rows.isEmpty() ? null : round((double) passes / rows.size()));
        rates.put("verifiedPassRate", rows.isEmpty() ? null : round((double) verified / rows.size()));
        return rates;
    }

    private static Map<String, Long> counts(final List<Map<String, Object>> rows, final String key,
                                            final Predicate<Map<String, Object>> filter) {
        final Map<String, Long> counts = new LinkedHashMap<>();
        rows.stream().filter(filter).map(r -> r.get(key)).filter(Objects::nonNull).
                forEach(v -> counts.merge(v.toString(), 1L, Long::sum));
        return counts;
    }

    private static Map<String, Object> slices(final List<Map<String, Object>> rows, final String key) {
        final Map<String, List<Map<String, Object>>> grouped = rows.stream().collect(Collectors.groupingBy(
                r -> String.valueOf(r.get(key)), LinkedHashMap::new, Collectors.toList()));
        final Map<String, Object> slices = new LinkedHashMap<>();
        grouped.forEach((slice, sliceRows) -> slices.put(slice, rates(sliceRows)));
        return slices;
    }

    /**
     * Totals and per-attempt means of what the attempts that reported usage used, or {@code null} when none did, as
     * usage that was not reported is unknown rather than zero.
     */
    private static Map<String, Object> usage(final List<Map<String, Object>> rows) {
        final List<Map<String, Object>> reported = rows.stream().map(r -> r.get("usage")).filter(Objects::nonNull).
                map(Snapshot::asMap).collect(Collectors.toList());
        if (reported.isEmpty()) return null;

        final Map<String, Long> totals = new LinkedHashMap<>();
        for (Map<String, Object> u : reported) {
            u.forEach((k, v) -> totals.merge(k, ((Number) v).longValue(), Long::sum));
        }
        final Map<String, Object> usage = new LinkedHashMap<>();
        usage.put("attemptsReporting", reported.size());
        usage.put("totals", totals);
        final Map<String, Object> means = new LinkedHashMap<>();
        totals.forEach((k, v) -> means.put(k, round((double) v / reported.size())));
        usage.put("perAttempt", means);
        return usage;
    }

    /**
     * How the attempts that reported their tool calls used each tool, or {@code null} when none reported, as calls
     * that were not reported are unknown rather than none. For each tool: the attempts that called it, its calls, and
     * how many of those attempts were verified passes. Attempts that called no tool are counted the same way, so a
     * tool's verified rate can be read against working without one.
     */
    private static Map<String, Object> tools(final List<Map<String, Object>> rows) {
        final List<Map<String, Object>> reported = rows.stream().filter(r -> r.get("tools") != null).
                collect(Collectors.toList());
        if (reported.isEmpty()) return null;

        final Map<String, Map<String, Long>> byTool = new TreeMap<>();
        final Map<String, Long> withoutTools = new LinkedHashMap<>();
        withoutTools.put("attempts", 0L);
        withoutTools.put("verified", 0L);
        long calls = 0;
        for (Map<String, Object> row : reported) {
            final List<Object> tools = list(row.get("tools"));
            final long verified = isVerified(row) ? 1 : 0;
            calls += tools.size();
            if (tools.isEmpty()) {
                withoutTools.merge("attempts", 1L, Long::sum);
                withoutTools.merge("verified", verified, Long::sum);
                continue;
            }
            for (Object tool : tools) {
                byTool.computeIfAbsent(tool.toString(), k -> new LinkedHashMap<>()).merge("calls", 1L, Long::sum);
            }
            for (Object tool : new LinkedHashSet<>(tools)) {
                final Map<String, Long> counts = byTool.get(tool.toString());
                counts.merge("attempts", 1L, Long::sum);
                counts.merge("verified", verified, Long::sum);
            }
        }
        final Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("attemptsReporting", reported.size());
        summary.put("callsPerAttempt", round((double) calls / reported.size()));
        summary.put("byTool", byTool);
        summary.put("withoutTools", withoutTools);
        return summary;
    }

    /**
     * Each distinct traversal with its outcome and differential finding, most frequent first, and the distinct
     * sequences of tool calls behind it when the driver reported them.
     */
    private static List<Map<String, Object>> traversals(final List<Map<String, Object>> rows) {
        final Map<List<Object>, Long> counts = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            counts.merge(Arrays.asList(row.get("traversal"), row.get("outcome"), row.get("differential")), 1L, Long::sum);
        }
        final List<Map<String, Object>> traversals = new ArrayList<>();
        counts.entrySet().stream().sorted(Map.Entry.<List<Object>, Long>comparingByValue().reversed()).forEach(e -> {
            final Map<String, Object> traversal = new LinkedHashMap<>();
            traversal.put("traversal", e.getKey().get(0));
            traversal.put("count", e.getValue());
            traversal.put("outcome", e.getKey().get(1));
            traversal.put("differential", e.getKey().get(2));
            final Map<List<Object>, Long> sequences = new LinkedHashMap<>();
            rows.stream().filter(r -> e.getKey().equals(Arrays.asList(r.get("traversal"), r.get("outcome"),
                    r.get("differential"))) && r.get("tools") != null).
                    forEach(r -> sequences.merge(list(r.get("tools")), 1L, Long::sum));
            if (!sequences.isEmpty()) {
                final List<Map<String, Object>> tools = new ArrayList<>();
                sequences.forEach((sequence, count) -> {
                    final Map<String, Object> calls = new LinkedHashMap<>();
                    calls.put("calls", sequence);
                    calls.put("count", count);
                    tools.add(calls);
                });
                traversal.put("tools", tools);
            }
            traversals.add(traversal);
        });
        return traversals;
    }

    /**
     * Each setup's difference from the setup with the {@code none} context that shares its model and harness, over
     * the scenarios both attempted. Setups without a model, such as the reference control, are not compared.
     */
    private static List<Map<String, Object>> comparisons(final List<Map<String, Object>> setups,
                                                         final Map<String, Map<String, List<Map<String, Object>>>> bySetup) {
        final List<Map<String, Object>> comparisons = new ArrayList<>();
        for (Map<String, Object> baseline : setups) {
            if (null == baseline.get("model") || !Collections.singletonList(NONE).equals(baseline.get("context"))) continue;
            for (Map<String, Object> setup : setups) {
                if (setup == baseline || !Objects.equals(setup.get("model"), baseline.get("model")) ||
                        !Objects.equals(setup.get("harness"), baseline.get("harness"))) continue;

                final Map<String, List<Map<String, Object>>> base = bySetup.getOrDefault(text(baseline, "name"), Collections.emptyMap());
                final Map<String, List<Map<String, Object>>> other = bySetup.getOrDefault(text(setup, "name"), Collections.emptyMap());
                final List<String> shared = other.keySet().stream().filter(base::containsKey).collect(Collectors.toList());
                if (shared.isEmpty()) continue;

                final Map<String, Object> comparison = new LinkedHashMap<>();
                comparison.put("model", setup.get("model"));
                comparison.put("harness", setup.get("harness"));
                comparison.put("baseline", baseline.get("name"));
                comparison.put("setup", setup.get("name"));
                comparison.put("context", setup.get("context"));
                comparison.put("scenarios", shared.size());
                comparison.put("verified", difference(shared, base, other, Snapshot::isVerified));
                comparison.put("raw", difference(shared, base, other, Snapshot::isPass));
                comparisons.add(comparison);
            }
        }
        return comparisons;
    }

    /**
     * The mean over scenarios of the setup's pass rate less the baseline's, with a 95% interval from a bootstrap that
     * resamples scenarios and then, within each, each side's attempts.
     */
    static Map<String, Object> difference(final List<String> scenarios,
                                          final Map<String, List<Map<String, Object>>> baseline,
                                          final Map<String, List<Map<String, Object>>> setup,
                                          final Predicate<Map<String, Object>> success) {
        final int n = scenarios.size();
        final boolean[][] base = new boolean[n][];
        final boolean[][] other = new boolean[n][];
        double observed = 0;
        for (int i = 0; i < n; i++) {
            base[i] = successes(baseline.get(scenarios.get(i)), success);
            other[i] = successes(setup.get(scenarios.get(i)), success);
            observed += rate(other[i]) - rate(base[i]);
        }

        final Random random = new Random(SEED);
        final double[] resampled = new double[RESAMPLES];
        for (int r = 0; r < RESAMPLES; r++) {
            double sum = 0;
            for (int i = 0; i < n; i++) {
                final int s = random.nextInt(n);
                sum += resampledRate(other[s], random) - resampledRate(base[s], random);
            }
            resampled[r] = sum / n;
        }
        Arrays.sort(resampled);

        final Map<String, Object> difference = new LinkedHashMap<>();
        difference.put("difference", round(observed / n));
        difference.put("interval", Arrays.asList(round(resampled[(int) Math.floor(0.025 * (RESAMPLES - 1))]),
                round(resampled[(int) Math.ceil(0.975 * (RESAMPLES - 1))])));
        return difference;
    }

    private static boolean[] successes(final List<Map<String, Object>> rows, final Predicate<Map<String, Object>> success) {
        final boolean[] successes = new boolean[rows.size()];
        for (int i = 0; i < successes.length; i++) {
            successes[i] = success.test(rows.get(i));
        }
        return successes;
    }

    private static double rate(final boolean[] successes) {
        int count = 0;
        for (boolean s : successes) {
            if (s) count++;
        }
        return (double) count / successes.length;
    }

    private static double resampledRate(final boolean[] successes, final Random random) {
        int count = 0;
        for (int i = 0; i < successes.length; i++) {
            if (successes[random.nextInt(successes.length)]) count++;
        }
        return (double) count / successes.length;
    }

    private static double round(final double value) {
        return Math.round(value * 10000) / 10000.0;
    }

    private static List<Map<String, Object>> flatten(final Map<String, List<Map<String, Object>>> byScenario) {
        if (null == byScenario) return Collections.emptyList();
        return byScenario.values().stream().flatMap(List::stream).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> records(final Map<String, Object> map, final String key) {
        final Object value = map.get(key);
        return null == value ? Collections.emptyList() : (List<Map<String, Object>>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(final Object value) {
        return null == value ? Collections.emptyList() : (List<Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(final Object value) {
        return (Map<String, Object>) value;
    }

    private static String text(final Map<String, Object> map, final String key) {
        return String.valueOf(map.get(key));
    }
}
