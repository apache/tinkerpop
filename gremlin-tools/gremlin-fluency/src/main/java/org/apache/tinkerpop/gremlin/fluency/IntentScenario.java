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

import java.util.Collections;
import java.util.List;

/**
 * A Gherkin scenario from the feature tests that carries an intent. It holds the scenario's source lines so that a
 * copy can be written with a candidate traversal in place of the reference one, which leaves the original feature
 * file untouched.
 */
public final class IntentScenario {
    private final String featurePath;
    private final String name;
    private final String graph;
    private final String intent;
    private final List<String> parameters;
    private final List<String> tags;
    private final String traversal;
    private final List<ScenarioStep> steps;
    private final List<String> sourceLines;
    private final int scenarioLine;
    private final int traversalOpen;
    private final int traversalClose;
    private final String hash;

    IntentScenario(final String featurePath, final String name, final String graph, final String intent,
                   final List<String> parameters, final List<String> tags, final String traversal,
                   final List<ScenarioStep> steps, final List<String> sourceLines, final int scenarioLine,
                   final int traversalOpen, final int traversalClose) {
        this.featurePath = featurePath;
        this.name = name;
        this.graph = graph;
        this.intent = intent;
        this.parameters = Collections.unmodifiableList(parameters);
        this.tags = Collections.unmodifiableList(tags);
        this.traversal = traversal;
        this.steps = Collections.unmodifiableList(steps);
        this.sourceLines = Collections.unmodifiableList(sourceLines);
        this.scenarioLine = scenarioLine;
        this.traversalOpen = traversalOpen;
        this.traversalClose = traversalClose;
        this.hash = Hashes.of(String.join("\n", sourceLines));
    }

    /**
     * The feature file the scenario came from, relative to the features root (e.g. {@code map/Vertex.feature}).
     */
    public String getFeaturePath() {
        return featurePath;
    }

    public String getName() {
        return name;
    }

    /**
     * The graph the scenario runs against as named in the feature file (e.g. {@code modern}, {@code airroutes},
     * {@code empty}).
     */
    public String getGraph() {
        return graph;
    }

    public String getIntent() {
        return intent;
    }

    /**
     * The names of the parameters the scenario declares, in declaration order.
     */
    public List<String> getParameters() {
        return parameters;
    }

    public List<String> getTags() {
        return tags;
    }

    /**
     * The scenario's own traversal, which is the reference a candidate is measured against.
     */
    public String getTraversal() {
        return traversal;
    }

    /**
     * The scenario's steps in order, each with its docstring, if it has one.
     */
    public List<ScenarioStep> getSteps() {
        return steps;
    }

    /**
     * Identifies the scenario's text, from its tags through its intent, traversal and expected results, so that
     * snapshots compare a scenario only when it has not changed between them.
     */
    public String getHash() {
        return hash;
    }

    /**
     * The scenario's source lines, starting with its tags, if any, and ending with its last step or table row.
     */
    List<String> getSourceLines() {
        return sourceLines;
    }

    /**
     * Index into {@link #getSourceLines()} of the {@code Scenario:} line.
     */
    int getScenarioLine() {
        return scenarioLine;
    }

    /**
     * Index into {@link #getSourceLines()} of the opening docstring delimiter of the traversal.
     */
    int getTraversalOpen() {
        return traversalOpen;
    }

    /**
     * Index into {@link #getSourceLines()} of the closing docstring delimiter of the traversal.
     */
    int getTraversalClose() {
        return traversalClose;
    }

    @Override
    public String toString() {
        return featurePath + ":" + name;
    }
}
