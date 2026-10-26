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

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class FeatureWriterTest {

    @Test
    public void shouldReplaceTraversalAndRenameScenario() {
        final IntentScenario scenario = CorpusTest.find("g_routesFromAUSToLASViaTexasHubs");
        final List<String> copy = FeatureWriter.copy(new Candidate(scenario, "a#1", "g.V().count()"));

        assertThat(copy.get(0).trim(), is("@GraphComputerVerificationReferenceOnly"));
        assertThat(copy, hasItem("  Scenario: a#1"));
        assertThat(copy, hasItem("      g.V().count()"));
        assertThat(copy, not(hasItem("      " + scenario.getTraversal())));
        assertThat(copy.size(), is(scenario.getSourceLines().size()));
    }

    @Test
    public void shouldIndentEveryLineOfAMultilineCandidate() {
        final IntentScenario scenario = CorpusTest.find("g_VX4X_bothE_otherV");
        final List<String> copy = FeatureWriter.copy(new Candidate(scenario, "a#1", "g.V(vid4).\n  bothE().\n  otherV()"));
        assertThat(copy, hasItem("      g.V(vid4)."));
        assertThat(copy, hasItem("        bothE()."));
        assertThat(copy.size(), is(scenario.getSourceLines().size() + 2));
    }

    @Test
    public void shouldEscapeDocStringDelimiterInCandidate() {
        final IntentScenario scenario = CorpusTest.find("g_VX4X_bothE_otherV");
        final List<String> copy = FeatureWriter.copy(new Candidate(scenario, "a#1", "g.V(vid4) \"\"\""));
        assertThat(copy, hasItem("      g.V(vid4) \\\"\\\"\\\""));
    }

    @Test
    public void shouldSkipCandidatesWithoutAnswer() {
        final IntentScenario scenario = CorpusTest.find("g_VX4X_bothE_otherV");
        assertThat(FeatureWriter.write(Arrays.asList(new Candidate(scenario, "a#1", null),
                new Candidate(scenario, "a#2", "  "))), is("Feature: gremlin-fluency attempts\n"));
    }
}
