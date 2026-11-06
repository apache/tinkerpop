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

import java.util.ArrayList;
import java.util.List;

/**
 * Writes copies of {@link IntentScenario} source with a {@link Candidate} traversal in place of the reference one.
 * Each copy is named after its attempt so that results can be traced back to it.
 */
final class FeatureWriter {
    private static final String DELIMITER = "\"\"\"";

    private FeatureWriter() {}

    /**
     * A feature file holding one scenario copy per candidate. Candidates without a traversal are skipped.
     */
    static String write(final List<Candidate> candidates) {
        final StringBuilder feature = new StringBuilder("Feature: gremlin-fluency attempts\n");
        for (Candidate candidate : candidates) {
            if (!candidate.hasTraversal()) continue;
            feature.append('\n');
            for (String line : copy(candidate)) {
                feature.append(line).append('\n');
            }
        }
        return feature.toString();
    }

    static List<String> copy(final Candidate candidate) {
        final IntentScenario scenario = candidate.getScenario();
        final List<String> source = scenario.getSourceLines();
        final List<String> lines = new ArrayList<>();

        for (int i = 0; i < scenario.getTraversalOpen() + 1; i++) {
            if (i == scenario.getScenarioLine()) {
                final String line = source.get(i);
                lines.add(line.substring(0, line.indexOf("Scenario:")) + "Scenario: " + candidate.getAttemptId());
            } else {
                lines.add(source.get(i));
            }
        }

        // gherkin strips the delimiter's indentation from docstring lines, so indent to match it. a delimiter inside
        // the candidate would end the docstring early, so escape it as gherkin allows
        final String open = source.get(scenario.getTraversalOpen());
        final String indent = open.substring(0, open.indexOf(DELIMITER));
        final String traversal = candidate.getTraversal().trim().replace(DELIMITER, "\\\"\\\"\\\"");
        for (String line : traversal.split("\\r?\\n", -1)) {
            lines.add(indent + line);
        }

        for (int i = scenario.getTraversalClose(); i < source.size(); i++) {
            lines.add(source.get(i));
        }
        return lines;
    }
}
