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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Reads asciidoc documentation for Gremlin snippets to help build the testing corpus.
 */
public class DocumentationReader {
    private static final Pattern sourceCodePattern = Pattern.compile("\\[gremlin-groovy.*\\].*");
    private static final Pattern startGremlinPattern = Pattern.compile("^g\\..*");

    /**
     * Some Gremlin in documentation is just not going to parse.
     */
    private static final Set<String> throwAway = new HashSet<>(Arrays.asList("g.inject(g.withComputer().V().shortestPath().with(ShortestPath.distance, 'weight').with(ShortestPath.includeEdges, true).with(ShortestPath.maxDistance, 1).toList().toArray()).map(unfold().values('name','weight').fold())"));

    public static Set<String> parse(final String projectRoot) throws IOException {
        final Set<String> gremlins = new LinkedHashSet<>();
        Files.find(Paths.get(projectRoot, "docs", "src"),
                Integer.MAX_VALUE,
                (filePath, fileAttr) -> fileAttr.isRegularFile() && (filePath.toString().endsWith("traversal.asciidoc") || filePath.toString().contains("recipes"))).
                sorted().
                forEach(f -> {
                    String currentGremlin = "";
                    int openSnippet = 0;

                    try {
                        final List<String> lines = Files.readAllLines(f, StandardCharsets.UTF_8);
                        for (String line : lines) {
                            // trim and remove callouts
                            String cleanLine = line.replaceAll("<\\d*>", "").trim();

                            // remove line comments
                            int pos = cleanLine.indexOf("//");
                            if (pos > 0) cleanLine = cleanLine.substring(0, pos).trim();

                            if (sourceCodePattern.matcher(cleanLine).matches() || (openSnippet > 0 && cleanLine.equals("----"))) {
                                if (openSnippet > 1) {
                                    openSnippet = 0;
                                } else {
                                    openSnippet++;
                                }
                            } else if (openSnippet > 0 && (startGremlinPattern.matcher(cleanLine).matches() || isTerminated(currentGremlin))) {
                                // new line of Gremlin that starts with g.
                                currentGremlin += cleanLine;

                                // line doesn't continue if there is no period or open paren
                                if (!isTerminated(currentGremlin)) {
                                    final String gremlinToAdd = replaceVariables(currentGremlin.trim());
                                    if (!throwAway.contains(gremlinToAdd))
                                        gremlins.add(gremlinToAdd);
                                    currentGremlin = "";
                                }
                            }
                        }
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                });

        return gremlins;
    }

    private static boolean isTerminated(final String gremlin) {
        return gremlin.endsWith(".") || gremlin.endsWith("(") || gremlin.endsWith(",");
    }

    /**
     * Variables can't be parsed by the grammar so they must be replaced with something concrete.
     */
    private static String replaceVariables(final String gremlin) {
        // convert lambda vars to closures as they will be ignored by the test suite
        return gremlin.replace("relations", "\"relations\"").
                replace("places.size()", "6").
                replace("places", "\"places\"").
                replace("ids", "\"ids\"").
                replace("vRexsterJob1", "\"rj1\"").
                replace("vBlueprintsJob1", "\"bj1\"").
                replace("weightFilter.clone()", "{it}").
                replace("weightFilter", "{it}").
                replace("vBob", "\"bob\"").
                replace("vMarko", "\"marko\"").
                replace("vPeter", "\"peter\"").
                replace("vStephen", "\"stephen\"").
                replace("input.head()", "\"head\"").
                replace("input.tail().size()", "6").
                replace("input.tail()", "\"tail\"").
                replace("System.out.&println", "{it}").
                replace("persons", "\"persons\"").
                replace("marko.value('age')", "11").
                replace("seedStrategy", "new SeedStrategy(seed: 99999)").
                replace(".getClass()", "").
                replace("result.toArray()", "4").
                replace("vA.value('amount')", "0.0").
                replace("vA", "\"vA\"");
    }
}
