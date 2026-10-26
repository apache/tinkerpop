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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class CorpusTest {
    private static final Corpus corpus = Corpus.fromClasspath();

    @Test
    public void shouldReadAnnotatedScenarioFromFeatureJar() {
        final IntentScenario scenario = find("g_VX4X_bothE_otherV");
        assertThat(scenario.getFeaturePath(), is("map/Edge.feature"));
        assertThat(scenario.getGraph(), is("modern"));
        assertThat(scenario.getIntent(), containsString("<vid4>"));
        assertThat(scenario.getParameters(), is(Collections.singletonList("vid4")));
        assertThat(scenario.getTraversal(), is("g.V(vid4).bothE().otherV()"));
    }

    @Test
    public void shouldReadEveryAirRoutesScenario() {
        final List<IntentScenario> airRoutes = corpus.getScenarios().stream().
                filter(s -> "airroutes".equals(s.getGraph())).collect(Collectors.toList());
        assertThat(airRoutes.size(), is(68));
        assertThat(find("g_routesFromAUSToLASViaTexasHubs").getTags(),
                is(Collections.singletonList("@GraphComputerVerificationReferenceOnly")));
    }

    @Test
    public void shouldExcludeScenarioWhoseIntentOmitsAParameter() {
        assertThat(corpus.getExclusions(), hasKey("g_VX1X_out_out_out"));
        assertThat(corpus.getScenarios().stream().anyMatch(s -> s.getName().equals("g_VX1X_out_out_out")), is(false));
    }

    @Test
    public void shouldIgnoreScenariosWithoutIntent() {
        assertThat(corpus.getScenarios().stream().anyMatch(s -> s.getName().equals("g_V_count")), is(false));
        assertThat(corpus.getExclusions(), not(hasKey("g_V_count")));
    }

    @Test
    public void shouldNotTakeTheNextScenarioCommentIntoTheBlock() {
        final IntentScenario scenario = find("g_distanceOfRouteFromDFWToMIA");
        final List<String> lines = scenario.getSourceLines();
        assertThat(lines.get(lines.size() - 1).trim(), is("| d[1120].i |"));
    }

    @Test
    public void shouldReadAWrappedIntentAsOneParagraph() {
        final List<IntentScenario> scenarios = new ArrayList<>();
        Corpus.read("x.feature", Arrays.asList(
                "Feature: x",
                "  Scenario: g_V_count",
                "    Given the modern graph",
                "    And the intent",
                "      \"\"\"",
                "      How many vertices",
                "      are in the graph?",
                "      \"\"\"",
                "    And the traversal of",
                "      \"\"\"",
                "      g.V().count()",
                "      \"\"\"",
                "    When iterated to list",
                "    Then the result should be unordered",
                "      | result |",
                "      | d[6].l |"), scenarios, new LinkedHashMap<>());
        assertThat(scenarios.get(0).getIntent(), is("How many vertices are in the graph?"));
    }

    @Test
    public void shouldExcludeScenarioThatRaisesAnError() {
        final List<IntentScenario> scenarios = new ArrayList<>();
        final Map<String, String> exclusions = new LinkedHashMap<>();
        Corpus.read("x.feature", Arrays.asList(
                "Feature: x",
                "  Scenario: g_fail",
                "    Given the empty graph",
                "    And the intent",
                "      \"\"\"",
                "      Fail.",
                "      \"\"\"",
                "    And the traversal of",
                "      \"\"\"",
                "      g.inject(1).fail()",
                "      \"\"\"",
                "    When iterated to list",
                "    Then the traversal will raise an error"), scenarios, exclusions);
        assertThat(scenarios.isEmpty(), is(true));
        assertThat(exclusions, hasKey("g_fail"));
    }

    @Test
    public void shouldHashEachScenarioByItsText() {
        final IntentScenario scenario = find("g_VX4X_bothE_otherV");
        assertThat(scenario.getHash(), is(find("g_VX4X_bothE_otherV").getHash()));
        assertThat(scenario.getHash().equals(find("g_VX1X_out").getHash()), is(false));
    }

    static IntentScenario find(final String name) {
        return corpus.getScenarios().stream().filter(s -> s.getName().equals(name)).findFirst().
                orElseThrow(() -> new AssertionError("No scenario " + name));
    }
}
