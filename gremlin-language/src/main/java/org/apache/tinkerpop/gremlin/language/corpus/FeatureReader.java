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
package org.apache.tinkerpop.gremlin.language.corpus;

import org.apache.commons.text.StringEscapeUtils;
import org.javatuples.Pair;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Reads the Gherkin feature files of {@code gremlin-test} and extracts Gremlin examples.
 */
public class FeatureReader {

    private static final Pattern generalParameterPattern = Pattern.compile("And using the parameter (.+) (defined as|of) (.*)");

    /**
     * Parses features to a {@code List} of Gremlin strings.
     *
     * @param featureDir The root directory where feature files can be found including subdirectories
     */
    public static List<String> parseFlat(final String featureDir) throws IOException {
        return parseFlat(featureDir, Collections.emptyList());
    }

    /**
     * Parses features to a {@code List} of Gremlin strings.
     *
     * @param featureDir The root directory where feature files can be found including subdirectories
     * @param parameterMatchers list of pattern/functions that will transform a parameter from its Gherkin form to
     *                          another format triggering that new formatted string to be inserted into the Gremlin
     *                          itself
     */
    public static List<String> parseFlat(final String featureDir, final List<Pair<Pattern, BiFunction<String, String, String>>> parameterMatchers) throws IOException {
        return parseGrouped(featureDir, parameterMatchers).values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    /**
     * Parses features to a {@code Map} structure of the test name as the key with a {@code List} of Gremlin strings
     * for the value.
     *
     * @param featureDir The root directory where feature files can be found including subdirectories
     */
    public static Map<String, List<String>> parseGrouped(final String featureDir) throws IOException {
        return parseGrouped(featureDir, Collections.emptyList());
    }

    /**
     * Parses features to a {@code Map} structure of the test name as the key with a {@code List} of Gremlin strings
     * for the value.
     *
     * @param featureDir The root directory where feature files can be found including subdirectories
     * @param parameterMatchers list of pattern/functions that will transform a parameter from its Gherkin form to
     *                          another format triggering that new formatted string to be inserted into the Gremlin
     *                          itself
     */
    public static Map<String, List<String>> parseGrouped(final String featureDir,
                                                         final List<Pair<Pattern, BiFunction<String, String, String>>> parameterMatchers) throws IOException {
        final Map<String, List<String>> gremlins = new LinkedHashMap<>();
        Files.find(Paths.get(featureDir),
                   Integer.MAX_VALUE,
                (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.toString().endsWith(".feature")).
                sorted().
                forEach(f -> {
                    String currentGremlin = "";
                    boolean openTriples = false;
                    boolean skipIgnored = false;
                    String scenarioName = "";
                    Map<String,String> parameters = new HashMap<>();

                    try {
                        final List<String> lines = Files.readAllLines(f, StandardCharsets.UTF_8);
                        for (String line : lines) {
                            String cleanLine = line.trim();
                            if (cleanLine.startsWith("Scenario:")) {
                                scenarioName = cleanLine.split(":")[1].trim();
                                skipIgnored = false;
                                parameters.clear();
                            } else if (!parameterMatchers.isEmpty() && cleanLine.startsWith("And using the parameter")) {
                                final Matcher m = generalParameterPattern.matcher(cleanLine);
                                if (m.matches()) {
                                    parameters.put(m.group(1), matchAndTransform(m.group(1), StringEscapeUtils.unescapeJava(m.group(3)), parameterMatchers));
                                } else {
                                    throw new IllegalStateException(String.format("Could not read parameters at: %s", cleanLine));
                                }
                            } else if (cleanLine.startsWith("Then nothing should happen because")) {
                                skipIgnored = true;
                            } else if (cleanLine.startsWith("And the graph should return")) {
                                gremlins.computeIfAbsent(scenarioName, k -> new ArrayList<>()).add(applyParametersToGremlin(StringEscapeUtils.unescapeJava(cleanLine.substring(cleanLine.indexOf("\"") + 1, cleanLine.lastIndexOf("\""))), parameters));
                            } else if (cleanLine.startsWith("\"\"\"")) {
                                openTriples = !openTriples;
                                if (!skipIgnored && !openTriples) {
                                    currentGremlin = applyParametersToGremlin(currentGremlin, parameters);
                                    gremlins.computeIfAbsent(scenarioName, k -> new ArrayList<>()).add(currentGremlin);
                                    currentGremlin = "";
                                }
                            } else if (openTriples && !skipIgnored) {
                                currentGremlin += cleanLine;
                            }
                        }
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                });

        return gremlins;
    }

    private static String applyParametersToGremlin(String currentGremlin, Map<String, String> parameters) {
        // sort from longest to shortest so that xx1 does not replace xx10
        final List<String> paramNames = new ArrayList<>(parameters.keySet());
        paramNames.sort((a,b) -> b.length() - a.length());
        for (String k : paramNames) {
            currentGremlin = currentGremlin.replace(k, parameters.get(k));
        }
        return currentGremlin;
    }

    private static String matchAndTransform(final String k, final String v,
                                            final List<Pair<Pattern, BiFunction<String, String, String>>> parameterMatchers) {
        for (Pair<Pattern,BiFunction<String,String,String>> matcherConverter : parameterMatchers) {
            final Pattern pattern = matcherConverter.getValue0();
            final Matcher matcher = pattern.matcher(v);
            if (matcher.find()) {
                final BiFunction<String,String,String> converter = matcherConverter.getValue1();
                // when there are no groups there is a direct match
                return converter.apply(k, matcher.groupCount() == 0 ? "" : matcher.group(1));
            }
        }

        // this should be a raw string if it didn't match anything - suppose it could be a syntax error in the
        // test too, but i guess the test would fail so perhaps ok to just assume it's raw string value that
        // didn't need a transform by default
        return v;
    }
}
