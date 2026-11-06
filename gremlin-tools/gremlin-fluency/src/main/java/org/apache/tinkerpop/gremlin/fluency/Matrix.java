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

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The setups a run compares and how it runs them, read from YAML. Relative driver commands and replay files resolve
 * against the directory of the matrix file.
 * <pre>
 * corpus:
 *   graphs: [modern, airroutes]       # optional, defaults to every annotated scenario
 * attempts: 3                         # each scenario is tried this many times per setup
 * parallelism: 4                      # attempts in flight at once, defaults to 1
 * timeouts: {driver: 120s, traversal: 30s}
 * differential: {seeds: [1, 2, 3]}    # perturbed graphs passing attempts are checked on, [] turns the check off
 * setups:
 *   - name: sonnet-none
 *     model: claude-sonnet-5-5
 *     harness: claude-code
 *     context: [none]
 *     driver: {command: [claude-code.sh], env: {MODEL: claude-sonnet-5-5}}
 *   - name: reference
 *     driver: {builtin: reference}
 * </pre>
 */
public final class Matrix {
    private static final Pattern DURATION = Pattern.compile("(\\d+)(ms|s|m)");

    private final List<String> graphs;
    private final int attempts;
    private final int parallelism;
    private final Duration driverTimeout;
    private final Duration traversalTimeout;
    private final List<Long> seeds;
    private final List<Setup> setups;

    private Matrix(final List<String> graphs, final int attempts, final int parallelism, final Duration driverTimeout,
                   final Duration traversalTimeout, final List<Long> seeds, final List<Setup> setups) {
        this.graphs = Collections.unmodifiableList(graphs);
        this.attempts = attempts;
        this.parallelism = parallelism;
        this.driverTimeout = driverTimeout;
        this.traversalTimeout = traversalTimeout;
        this.seeds = Collections.unmodifiableList(seeds);
        this.setups = Collections.unmodifiableList(setups);
    }

    /**
     * The graphs to draw scenarios from, empty meaning all of them.
     */
    public List<String> getGraphs() {
        return graphs;
    }

    public int getAttempts() {
        return attempts;
    }

    /**
     * How many attempts a setup has in flight at once.
     */
    public int getParallelism() {
        return parallelism;
    }

    public Duration getDriverTimeout() {
        return driverTimeout;
    }

    public Duration getTraversalTimeout() {
        return traversalTimeout;
    }

    /**
     * The seeds of the perturbed graphs that the differential pass checks passing attempts on.
     */
    public List<Long> getSeeds() {
        return seeds;
    }

    public List<Setup> getSetups() {
        return setups;
    }

    public static Matrix load(final Path file) throws IOException {
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            final Path base = file.toAbsolutePath().getParent();
            return fromMap(new Yaml().load(reader), base);
        }
    }

    @SuppressWarnings("unchecked")
    static Matrix fromMap(final Map<String, Object> map, final Path base) {
        if (null == map) throw new IllegalArgumentException("The matrix is empty");

        final Map<String, Object> corpus = (Map<String, Object>) map.getOrDefault("corpus", Collections.emptyMap());
        final List<String> graphs = new ArrayList<>((List<String>) corpus.getOrDefault("graphs", Collections.emptyList()));
        final int attempts = ((Number) map.getOrDefault("attempts", 1)).intValue();
        if (attempts < 1) throw new IllegalArgumentException("attempts must be at least 1");
        final int parallelism = ((Number) map.getOrDefault("parallelism", 1)).intValue();
        if (parallelism < 1) throw new IllegalArgumentException("parallelism must be at least 1");

        final Map<String, Object> timeouts = (Map<String, Object>) map.getOrDefault("timeouts", Collections.emptyMap());
        final Duration driverTimeout = duration(timeouts.getOrDefault("driver", "120s"));
        final Duration traversalTimeout = duration(timeouts.getOrDefault("traversal", "30s"));

        final Map<String, Object> differential = (Map<String, Object>) map.getOrDefault("differential", Collections.emptyMap());
        final List<Long> seeds = ((List<Number>) differential.getOrDefault("seeds", Arrays.asList(1, 2, 3))).stream().
                map(Number::longValue).collect(Collectors.toList());

        final List<Setup> setups = new ArrayList<>();
        for (Map<String, Object> s : (List<Map<String, Object>>) map.getOrDefault("setups", Collections.emptyList())) {
            final Map<String, Object> driver = (Map<String, Object>) s.getOrDefault("driver", Collections.emptyMap());
            final List<String> command = new ArrayList<>((List<String>) driver.getOrDefault("command", Collections.emptyList()));

            // a relative path to the executable means a script beside the matrix, while a bare name is left for the
            // PATH to find
            if (!command.isEmpty() && command.get(0).contains("/") && !Path.of(command.get(0)).isAbsolute())
                command.set(0, base.resolve(command.get(0)).normalize().toString());

            final Object file = driver.get("file");
            setups.add(new Setup((String) s.get("name"), (String) s.get("model"), (String) s.get("harness"),
                    (List<String>) s.getOrDefault("context", Collections.emptyList()),
                    Boolean.TRUE.equals(s.get("server")), command,
                    (Map<String, String>) driver.getOrDefault("env", Collections.emptyMap()),
                    (String) driver.get("builtin"), null == file ? null : base.resolve(file.toString()).normalize()));
        }
        if (setups.isEmpty()) throw new IllegalArgumentException("The matrix has no setups");
        if (setups.stream().map(Setup::getName).distinct().count() != setups.size())
            throw new IllegalArgumentException("Setup names must be unique");

        return new Matrix(graphs, attempts, parallelism, driverTimeout, traversalTimeout, seeds, setups);
    }

    static Duration duration(final Object value) {
        final Matcher m = DURATION.matcher(value.toString().trim());
        if (!m.matches()) throw new IllegalArgumentException("Durations look like 500ms, 30s or 2m: " + value);
        final long amount = Long.parseLong(m.group(1));
        switch (m.group(2)) {
            case "ms":
                return Duration.ofMillis(amount);
            case "s":
                return Duration.ofSeconds(amount);
            default:
                return Duration.ofMinutes(amount);
        }
    }
}
