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
package org.apache.tinkerpop.gremlin.language.grammar;

import org.apache.commons.text.StringEscapeUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Reads the feature files and extracts Gremlin to help build the parsing test corpus.
 */
public class FeatureReader {

    public static Set<String> parse(final String projectRoot) throws IOException {
        final Set<String> gremlins = new LinkedHashSet<>();
        Files.find(Paths.get(projectRoot, "gremlin-test", "features"),
                   Integer.MAX_VALUE,
                (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.toString().endsWith(".feature")).
                sorted().
                forEach(f -> {
                    String currentGremlin = "";
                    boolean openTriples = false;
                    boolean skipIgnored = false;

                    try {
                        final List<String> lines = Files.readAllLines(f, StandardCharsets.UTF_8);
                        for (String line : lines) {
                            String cleanLine = line.trim();
                            if (cleanLine.startsWith("Then nothing should happen because")) {
                                skipIgnored = true;
                            } else if (cleanLine.startsWith("And the graph should return")) {
                                gremlins.add(replaceVariables(
                                        StringEscapeUtils.unescapeJava(
                                                cleanLine.substring(cleanLine.indexOf("\"") + 1, cleanLine.lastIndexOf("\"")))));
                            } else if (cleanLine.startsWith("\"\"\"")) {
                                openTriples = !openTriples;
                                if (!skipIgnored && !openTriples) {
                                    gremlins.add(replaceVariables(currentGremlin));
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

    /**
     * Variables can't be parsed by the grammar so they must be replaced with something concrete.
     */
    private static String replaceVariables(final String gremlin) {
        return gremlin.replace("xx1", "\"1\"").
                replace("xx2", "\"2\"").
                replace("xx3", "\"3\"").
                replace("vid1", "1").
                replace("vid2", "2").
                replace("vid3", "3").
                replace("vid4", "4").
                replace("vid5", "5").
                replace("vid6", "6").
                replace("eid7", "7").
                replace("eid8", "8").
                replace("eid9", "9").
                replace("eid10", "10").
                replace("eid11", "11").
                replace("eid12", "12");
    }
}
