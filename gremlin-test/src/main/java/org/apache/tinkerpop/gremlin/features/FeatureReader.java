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
package org.apache.tinkerpop.gremlin.features;

import org.apache.commons.text.StringEscapeUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads the feature files and extracts Gremlin to a {@code Map} structure that can be used to then generate
 */
public class FeatureReader {

    public static Map<String, List<String>> parse(final String projectRoot) throws IOException {
        final Map<String, List<String>> gremlins = new LinkedHashMap<>();
        Files.find(Paths.get(projectRoot, "gremlin-test", "features"),
                   Integer.MAX_VALUE,
                (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.toString().endsWith(".feature")).
                sorted().
                forEach(f -> {
                    String currentGremlin = "";
                    boolean openTriples = false;
                    boolean skipIgnored = false;
                    String scenarioName = "";

                    try {
                        final List<String> lines = Files.readAllLines(f, StandardCharsets.UTF_8);
                        for (String line : lines) {
                            String cleanLine = line.trim();
                            if (cleanLine.startsWith("Scenario:")) {
                                scenarioName = cleanLine.split(":")[1].trim();
                                skipIgnored = false;
                            } else if (cleanLine.startsWith("Then nothing should happen because")) {
                                skipIgnored = true;
                            } else if (cleanLine.startsWith("And the graph should return")) {
                                gremlins.computeIfAbsent(scenarioName, k -> new ArrayList<>()).add(StringEscapeUtils.unescapeJava(cleanLine.substring(cleanLine.indexOf("\"") + 1, cleanLine.lastIndexOf("\""))));
                            } else if (cleanLine.startsWith("\"\"\"")) {
                                openTriples = !openTriples;
                                if (!skipIgnored && !openTriples) {
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
}
