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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Runs an external command for each prompt, which is how agent harnesses are driven. The command reads the prompt on
 * stdin and writes the traversal on stdout. It starts in a fresh, empty working directory so that no TinkerPop
 * checkout is within reach, and it receives an attempt number that does not reveal the scenario. A setup with a
 * server also receives where the scenario's graph is served. The graph is never named, so that a driver cannot pass
 * its name on to the agent.
 * A non-zero exit, empty output or exceeding the timeout means no answer. A command may report its version by
 * writing a {@code fluency-version: ...} line to stderr, and what it used, as whole numbers, with a
 * {@code fluency-usage: input_tokens=... output_tokens=... turns=...} line, and the tools the agent called, in order and
 * separated by spaces, with a {@code fluency-tools: ...} line, which is left empty when it called none.
 */
public final class CommandDriver implements Driver {
    public static final String ENV_ATTEMPT = "FLUENCY_ATTEMPT";
    public static final String ENV_GREMLIN_ENDPOINT = "FLUENCY_GREMLIN_ENDPOINT";

    private static final Pattern FENCE = Pattern.compile("^```[\\w-]*[ \\t]*\\R(.*?)\\R?```$", Pattern.DOTALL);
    private static final Pattern BACKTICKS = Pattern.compile("^`([^`]+)`$", Pattern.DOTALL);
    private static final Pattern VERSION = Pattern.compile("^fluency-version:\\s*(.+)$", Pattern.MULTILINE);
    private static final Pattern USAGE = Pattern.compile("^fluency-usage:\\s*(.+)$", Pattern.MULTILINE);
    private static final Pattern USAGE_ENTRY = Pattern.compile("(\\w+)=(\\d+)");
    private static final Pattern TOOLS = Pattern.compile("^fluency-tools:[ \\t]*(.*)$", Pattern.MULTILINE);
    private static final int MAX_DETAIL_LENGTH = 2000;

    private final List<String> command;
    private final Map<String, String> environment;
    private final Duration timeout;
    private final Function<String, String> endpoints;
    private volatile String version;

    public CommandDriver(final List<String> command, final Map<String, String> environment, final Duration timeout) {
        this(command, environment, timeout, null);
    }

    /**
     * @param endpoints maps a graph name to where it is served, given to the command as
     *                  {@code FLUENCY_GREMLIN_ENDPOINT}, or {@code null} when the command gets no server
     */
    public CommandDriver(final List<String> command, final Map<String, String> environment, final Duration timeout,
                         final Function<String, String> endpoints) {
        if (command.isEmpty()) throw new IllegalArgumentException("command must not be empty");
        this.command = new ArrayList<>(command);
        this.environment = new HashMap<>(environment);
        this.timeout = timeout;
        this.endpoints = endpoints;
    }

    /**
     * The version the command last reported, or {@code null} if it never did.
     */
    public String getVersion() {
        return version;
    }

    @Override
    public Answer answer(final IntentScenario scenario, final String prompt, final int attempt) {
        final long start = System.nanoTime();
        Path workingDirectory = null;
        try {
            workingDirectory = Files.createTempDirectory("agent-");
            final ProcessBuilder builder = new ProcessBuilder(command).directory(workingDirectory.toFile());
            builder.environment().putAll(environment);
            builder.environment().put(ENV_ATTEMPT, String.valueOf(attempt));
            if (endpoints != null) builder.environment().put(ENV_GREMLIN_ENDPOINT, endpoints.apply(scenario.getGraph()));

            final Process process = builder.start();
            final CompletableFuture<String> stdout = drain(process.getInputStream());
            final CompletableFuture<String> stderr = drain(process.getErrorStream());
            try (OutputStream stdin = process.getOutputStream()) {
                stdin.write(prompt.getBytes(StandardCharsets.UTF_8));
            } catch (IOException ex) {
                // the command may exit without reading its input, which its exit code will explain
            }

            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                return new Answer(null, "driver timed out after " + timeout, elapsed(start));
            }

            final String err = stderr.join();
            final Matcher versionMatcher = VERSION.matcher(err);
            if (versionMatcher.find()) version = versionMatcher.group(1).trim();
            final Map<String, Long> usage = usage(err);
            final List<String> tools = tools(err);

            if (process.exitValue() != 0)
                return new Answer(null, "driver exited with " + process.exitValue() + ": " + tail(err), elapsed(start),
                        usage, tools);

            final String traversal = clean(stdout.join());
            return new Answer(traversal.isEmpty() ? null : traversal, err.isEmpty() ? null : tail(err), elapsed(start),
                    usage, tools);
        } catch (IOException ex) {
            return new Answer(null, "driver could not run: " + ex.getMessage(), elapsed(start));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return new Answer(null, "interrupted", elapsed(start));
        } finally {
            if (workingDirectory != null) delete(workingDirectory);
        }
    }

    /**
     * Removes one surrounding Markdown code fence, or one surrounding pair of inline backticks, and trims whitespace.
     * Nothing else is repaired, because writing canonical gremlin-lang is part of what is measured.
     */
    static String clean(final String output) {
        final String trimmed = output.trim();
        final Matcher fence = FENCE.matcher(trimmed);
        if (fence.matches()) return fence.group(1).trim();

        final Matcher backticks = BACKTICKS.matcher(trimmed);
        return backticks.matches() ? backticks.group(1).trim() : trimmed;
    }

    /**
     * The entries of the last {@code fluency-usage} line in the command's stderr.
     */
    static Map<String, Long> usage(final String stderr) {
        final Map<String, Long> usage = new LinkedHashMap<>();
        final Matcher line = USAGE.matcher(stderr);
        String last = null;
        while (line.find()) last = line.group(1);
        if (last != null) {
            final Matcher entry = USAGE_ENTRY.matcher(last);
            while (entry.find()) usage.put(entry.group(1), Long.parseLong(entry.group(2)));
        }
        return usage;
    }

    /**
     * The tools named by the last {@code fluency-tools} line in the command's stderr, or {@code null} if there is none.
     */
    static List<String> tools(final String stderr) {
        final Matcher line = TOOLS.matcher(stderr);
        String last = null;
        while (line.find()) last = line.group(1).trim();
        if (null == last) return null;
        return last.isEmpty() ? Collections.emptyList() : Arrays.asList(last.split("\\s+"));
    }

    private static CompletableFuture<String> drain(final InputStream in) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        });
    }

    private static String tail(final String text) {
        final String trimmed = text.trim();
        return trimmed.length() > MAX_DETAIL_LENGTH ? "..." + trimmed.substring(trimmed.length() - MAX_DETAIL_LENGTH) : trimmed;
    }

    private static long elapsed(final long start) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

    private static void delete(final Path directory) {
        try (Stream<Path> paths = Files.walk(directory)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        } catch (IOException ex) {
            // a leftover temporary directory is harmless
        }
    }
}
