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
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The scenarios from the Gherkin feature tests that carry an intent. Feature files are read from the gremlin-test
 * jar on the classpath and are never modified. Scenarios that carry an intent but cannot be scored fairly are kept
 * aside as exclusions with a reason, so that they are reported rather than silently dropped.
 */
public final class Corpus {
    public static final String FEATURES_ROOT = "org/apache/tinkerpop/gremlin/test/features";

    private static final String DELIMITER = "\"\"\"";
    private static final Pattern STEP = Pattern.compile("^(Given|When|Then|And|But) (.*)$");
    private static final Pattern GRAPH = Pattern.compile("^the (\\w+) graph$");
    private static final Pattern PARAMETER = Pattern.compile("^using the parameter (\\w+) defined as .*$");

    private final List<IntentScenario> scenarios;
    private final Map<String, String> exclusions;

    private Corpus(final List<IntentScenario> scenarios, final Map<String, String> exclusions) {
        this.scenarios = Collections.unmodifiableList(scenarios);
        this.exclusions = Collections.unmodifiableMap(exclusions);
    }

    /**
     * Scenarios that carry an intent and can be scored.
     */
    public List<IntentScenario> getScenarios() {
        return scenarios;
    }

    /**
     * Scenarios that carry an intent but cannot be scored, keyed by scenario name, with the reason.
     */
    public Map<String, String> getExclusions() {
        return exclusions;
    }

    /**
     * Reads the feature files from the gremlin-test jar, or directory, on the classpath.
     */
    public static Corpus fromClasspath() {
        return fromClasspathDirectory(FEATURES_ROOT, Corpus::fromDirectory);
    }

    /**
     * Reads a directory of resources from the jar, or directory, on the classpath that holds it.
     */
    static <T> T fromClasspathDirectory(final String root, final DirectoryReader<T> reader) {
        final URL url = Corpus.class.getClassLoader().getResource(root);
        if (null == url)
            throw new IllegalStateException("Nothing found on the classpath at " + root);

        try {
            final URI uri = url.toURI();
            if (uri.getScheme().equals("jar")) {
                FileSystem fs;
                boolean created = false;
                try {
                    fs = FileSystems.newFileSystem(uri, Collections.emptyMap());
                    created = true;
                } catch (FileSystemAlreadyExistsException ex) {
                    fs = FileSystems.getFileSystem(uri);
                }
                try {
                    return reader.read(fs.getPath(root));
                } finally {
                    if (created) fs.close();
                }
            } else {
                return reader.read(Paths.get(uri));
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        } catch (URISyntaxException ex) {
            throw new IllegalStateException(ex);
        }
    }

    interface DirectoryReader<T> {
        T read(final Path directory) throws IOException;
    }

    /**
     * Reads every {@code .feature} file below the given directory.
     */
    public static Corpus fromDirectory(final Path root) throws IOException {
        final List<IntentScenario> scenarios = new ArrayList<>();
        final Map<String, String> exclusions = new LinkedHashMap<>();
        final List<Path> features;
        try (Stream<Path> paths = Files.walk(root)) {
            features = paths.filter(p -> p.toString().endsWith(".feature")).sorted().collect(Collectors.toList());
        }

        for (Path feature : features) {
            final String featurePath = root.relativize(feature).toString().replace('\\', '/');
            final List<String> lines = Files.readAllLines(feature, StandardCharsets.UTF_8);
            read(featurePath, lines, scenarios, exclusions);
        }

        return new Corpus(scenarios, exclusions);
    }

    static void read(final String featurePath, final List<String> lines, final List<IntentScenario> scenarios,
                     final Map<String, String> exclusions) {
        final List<Integer> starts = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).trim().startsWith("Scenario:")) starts.add(i);
        }

        for (int s = 0; s < starts.size(); s++) {
            final int scenarioLine = starts.get(s);
            final int first = firstTagLine(lines, scenarioLine);
            int end = s + 1 < starts.size() ? firstTagLine(lines, starts.get(s + 1)) : lines.size();

            // comments and blank lines ahead of the next scenario belong to it rather than to this one
            while (end > scenarioLine + 1 && (lines.get(end - 1).trim().isEmpty() || lines.get(end - 1).trim().startsWith("#")))
                end--;

            readScenario(featurePath, new ArrayList<>(lines.subList(first, end)), scenarioLine - first,
                    scenarios, exclusions);
        }
    }

    private static int firstTagLine(final List<String> lines, final int scenarioLine) {
        int first = scenarioLine;
        while (first > 0 && lines.get(first - 1).trim().startsWith("@")) first--;
        return first;
    }

    private static void readScenario(final String featurePath, final List<String> block, final int scenarioLine,
                                     final List<IntentScenario> scenarios, final Map<String, String> exclusions) {
        final String name = block.get(scenarioLine).trim().substring("Scenario:".length()).trim();
        final List<String> tags = new ArrayList<>();
        for (int i = 0; i < scenarioLine; i++) {
            tags.addAll(Arrays.asList(block.get(i).trim().split("\\s+")));
        }

        String graph = null;
        String intent = null;
        String traversal = null;
        int traversalOpen = -1;
        int traversalClose = -1;
        boolean raisesError = false;
        final List<String> parameters = new ArrayList<>();
        final List<ScenarioStep> steps = new ArrayList<>();

        String previousStep = null;
        for (int i = scenarioLine + 1; i < block.size(); i++) {
            final String line = block.get(i).trim();
            if (line.equals(DELIMITER)) {
                final int close = closingDelimiter(block, i);
                final String docString = docString(block, i, close);
                if (!steps.isEmpty()) steps.set(steps.size() - 1, steps.get(steps.size() - 1).withDocString(docString));
                // an intent is one paragraph wrapped to the width of the feature file
                if ("the intent".equals(previousStep))
                    intent = docString.trim().replaceAll("\\s*\\R\\s*", " ");
                else if ("the traversal of".equals(previousStep)) {
                    traversal = docString;
                    traversalOpen = i;
                    traversalClose = close;
                }
                i = close;
                continue;
            }

            final Matcher step = STEP.matcher(line);
            if (!step.matches()) continue;
            previousStep = step.group(2);
            steps.add(new ScenarioStep(previousStep, null));

            final Matcher graphMatcher = GRAPH.matcher(previousStep);
            if (graphMatcher.matches() && null == graph) graph = graphMatcher.group(1);

            final Matcher parameterMatcher = PARAMETER.matcher(previousStep);
            if (parameterMatcher.matches()) parameters.add(parameterMatcher.group(1));

            if (previousStep.startsWith("the traversal will raise an error")) raisesError = true;
        }

        // only scenarios with an intent are part of the corpus
        if (null == intent) return;

        if (null == traversal) {
            exclusions.put(name, "the scenario has no traversal");
            return;
        }
        if (raisesError) {
            exclusions.put(name, "the scenario expects the traversal to raise an error");
            return;
        }
        for (String parameter : parameters) {
            if (!intent.contains("<" + parameter + ">")) {
                exclusions.put(name, "the intent does not mention parameter <" + parameter + ">");
                return;
            }
        }

        scenarios.add(new IntentScenario(featurePath, name, graph, intent, parameters, tags, traversal, steps,
                block, scenarioLine, traversalOpen, traversalClose));
    }

    private static int closingDelimiter(final List<String> block, final int open) {
        for (int i = open + 1; i < block.size(); i++) {
            if (block.get(i).trim().equals(DELIMITER)) return i;
        }
        throw new IllegalStateException("Unclosed docstring at line " + open + " of " + block.get(0));
    }

    /**
     * Gherkin removes the opening delimiter's indentation from each line of a docstring.
     */
    private static String docString(final List<String> block, final int open, final int close) {
        final int indent = block.get(open).indexOf(DELIMITER);
        final List<String> content = new ArrayList<>();
        for (int i = open + 1; i < close; i++) {
            final String line = block.get(i);
            int strip = 0;
            while (strip < indent && strip < line.length() && line.charAt(strip) == ' ') strip++;
            content.add(line.substring(strip));
        }
        return String.join("\n", content);
    }
}
