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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DifferentialTest {
    private static final List<Long> SEEDS = Arrays.asList(1L, 2L, 3L);

    private static ScoredAttempt check(final String scenarioName, final String traversal) {
        final IntentScenario scenario = CorpusTest.find(scenarioName);
        return new Differential(SEEDS, Duration.ofSeconds(30)).check(
                new ScoredAttempt(new Candidate(scenario, scenarioName + "#test", traversal), Outcome.PASS, null));
    }

    @Test
    public void shouldNeverFlagTheReferenceItself() {
        final Differential differential = new Differential(SEEDS, Duration.ofSeconds(60));
        final List<ScoredAttempt> checked = Corpus.fromClasspath().getScenarios().stream().
                map(s -> differential.check(new ScoredAttempt(new Candidate(s, s.getName() + "#reference", s.getTraversal()),
                        Outcome.PASS, null))).
                collect(Collectors.toList());

        // every scenario is either compared and agrees, or is not checked only because nothing can be compared
        final List<ScoredAttempt> unexpected = checked.stream().
                filter(a -> a.getDifferential() != DifferentialOutcome.AGREES).
                filter(a -> a.getDifferential() != DifferentialOutcome.NOT_CHECKED ||
                        Differential.compareResults(a.getCandidate().getScenario())).
                collect(Collectors.toList());
        assertThat(unexpected.toString(), unexpected.isEmpty(), is(true));
    }

    @Test
    public void shouldAgreeWithAnEquivalentTraversal() {
        final ScoredAttempt attempt = check("g_VX1X_out", "g.V(vid1).outE().inV()");
        assertThat(attempt.toString(), attempt.getDifferential(), is(DifferentialOutcome.AGREES));
    }

    @Test
    public void shouldCatchAPassThatDependsOnTheData() {
        // marko has no incoming edges in modern, so both() returns what out() does until one is added
        final ScoredAttempt attempt = check("g_VX1X_out", "g.V(vid1).both()");
        assertThat(attempt.toString(), attempt.getDifferential(), is(DifferentialOutcome.COINCIDENTAL));
    }

    @Test
    public void shouldCatchAnEdgeWrittenWithoutItsProperty() {
        // the scenario never asserts the weight, so this candidate passes scoring
        final ScoredAttempt attempt = check("g_addEXknowsX_fromXVXvid1XX_toXVXvid6XX_propertyXweight_0_1X",
                "g.addE(\"knows\").from(__.V(vid1)).to(__.V(vid6))");
        assertThat(attempt.toString(), attempt.getDifferential(), is(DifferentialOutcome.STATE_MISMATCH));
    }

    @Test
    public void shouldCatchAVertexWrittenWithAnExtraProperty() {
        final ScoredAttempt attempt = check("g_addVXpersonX_propertyXname_stephenX",
                "g.addV(\"person\").property(\"name\", \"stephen\").property(\"age\", 29)");
        assertThat(attempt.toString(), attempt.getDifferential(), is(DifferentialOutcome.STATE_MISMATCH));
    }

    @Test
    public void shouldAgreeWithAMutationWrittenAnotherWay() {
        final ScoredAttempt attempt = check("g_addEXknowsX_fromXVXvid1XX_toXVXvid6XX_propertyXweight_0_1X",
                "g.V(vid6).as(\"p\").V(vid1).addE(\"knows\").to(\"p\").property(\"weight\", xx1)");
        assertThat(attempt.toString(), attempt.getDifferential(), is(DifferentialOutcome.AGREES));
    }

    @Test
    public void shouldLeaveAttemptsThatDidNotPassAlone() {
        final IntentScenario scenario = CorpusTest.find("g_VX1X_out");
        final ScoredAttempt attempt = new Differential(SEEDS, Duration.ofSeconds(30)).check(
                new ScoredAttempt(new Candidate(scenario, "wrong", "g.V(vid1).in()"), Outcome.WRONG_RESULT, "no"));
        assertThat(attempt.getDifferential(), is(nullValue()));
    }

    @Test
    public void shouldNotCheckWithoutSeeds() {
        final IntentScenario scenario = CorpusTest.find("g_VX1X_out");
        final ScoredAttempt attempt = new Differential(Arrays.asList(), Duration.ofSeconds(30)).check(
                new ScoredAttempt(new Candidate(scenario, "both", "g.V(vid1).both()"), Outcome.PASS, null));
        assertThat(attempt.getDifferential(), is(DifferentialOutcome.NOT_CHECKED));
    }

    @Test
    public void shouldCompareResultsUnlessOnlyTheirSizeIsAsserted() {
        assertThat(Differential.compareResults(CorpusTest.find("g_VX1X_out")), is(true));
        assertThat(Differential.compareResults(CorpusTest.find("g_addVXpersonX_propertyXname_stephenX")), is(false));
    }
}
