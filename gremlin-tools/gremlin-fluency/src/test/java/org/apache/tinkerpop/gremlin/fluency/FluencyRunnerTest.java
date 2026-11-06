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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class FluencyRunnerTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldRunEachSetupOverTheCorpus() throws Exception {
        final Path conf = folder.newFolder("conf").toPath();
        Files.write(conf.resolve("answers.yaml"), "g_VX4X_bothE_otherV: g.V(vid4).out()\n".getBytes(StandardCharsets.UTF_8));

        // a stand-in for an agent wrapper: it answers every prompt with the same traversal
        final Path agent = CommandDriverTest.script(conf, "cat > /dev/null\necho 'g.V(vid4).bothE().otherV()'\n");
        Files.write(conf.resolve("matrix.yaml"), String.join("\n",
                "corpus: {graphs: [modern]}",
                "attempts: 2",
                "parallelism: 3",
                "setups:",
                "  - name: reference",
                "    driver: {builtin: reference}",
                "  - name: replay",
                "    driver: {builtin: replay, file: answers.yaml}",
                "  - name: agent",
                "    model: none",
                "    harness: script",
                "    context: [none]",
                "    driver: {command: [" + agent + "]}").getBytes(StandardCharsets.UTF_8));

        final Path workDir = folder.newFolder("run").toPath();
        final Corpus corpus = Corpus.fromClasspath();
        final List<ScoredAttempt> attempts = FluencyRunner.run("test", Matrix.load(conf.resolve("matrix.yaml")), corpus, workDir);

        final long modernScenarios = corpus.getScenarios().stream().filter(s -> "modern".equals(s.getGraph())).count();
        assertThat((long) attempts.size(), is(modernScenarios * 2 * 3));

        // attempts made in parallel still come back in corpus order
        final List<String> expectedOrder = corpus.getScenarios().stream().filter(s -> "modern".equals(s.getGraph())).
                flatMap(s -> Stream.of(s.getName() + "#agent#1", s.getName() + "#agent#2")).
                collect(Collectors.toList());
        assertThat(attempts.stream().map(a -> a.getCandidate().getAttemptId()).filter(id -> id.contains("#agent#")).
                collect(Collectors.toList()), is(expectedOrder));

        final Map<String, List<Outcome>> bySetup = attempts.stream().collect(Collectors.groupingBy(
                a -> a.getCandidate().getAttemptId().split("#")[1],
                Collectors.mapping(ScoredAttempt::getOutcome, Collectors.toList())));
        assertThat(bySetup.get("reference").stream().allMatch(o -> o == Outcome.PASS), is(true));

        // the replay file answers one scenario wrongly and leaves the rest unanswered
        assertThat(bySetup.get("replay").stream().filter(o -> o == Outcome.WRONG_RESULT).count(), is(2L));
        assertThat(bySetup.get("replay").stream().filter(o -> o == Outcome.NO_ANSWER).count(), is((modernScenarios - 1) * 2));

        // the agent's one answer is right for its scenario and wrong or broken elsewhere
        assertThat(attempts.stream().filter(a -> a.getCandidate().getAttemptId().startsWith("g_VX4X_bothE_otherV#agent#")).
                allMatch(a -> a.getOutcome() == Outcome.PASS), is(true));

        final List<String> rows = Files.readAllLines(workDir.resolve("attempts.jsonl"), StandardCharsets.UTF_8);
        assertThat((long) rows.size(), is(modernScenarios * 2 * 3));
        assertThat(rows.get(0), containsString("\"setup\":\"reference\""));
        assertThat(rows.get(0), containsString("\"differential\":\"AGREES\""));

        final String run = new String(Files.readAllBytes(workDir.resolve("run.json")), StandardCharsets.UTF_8);
        assertThat(run, containsString("\"promptHash\""));
        assertThat(run, containsString("\"seeds\""));
        assertThat(run, containsString("g_VX1X_out_out_out"));
        assertThat(run, containsString("\"corpusHash\""));
        assertThat(run, containsString("\"git\""));
        assertThat(run, containsString("\"hash\" : \"" + CorpusTest.find("g_VX1X_out").getHash() + "\""));

        final String snapshot = new String(Files.readAllBytes(workDir.resolve("snapshot.json")), StandardCharsets.UTF_8);
        assertThat(snapshot, containsString("\"verifiedPassRate\" : 1.0"));
        assertThat(Files.isDirectory(workDir.resolve("agent").resolve("features")), is(true));
    }

    @Test
    public void shouldGiveServerSetupsTheirScenarioGraph() throws Exception {
        final Path conf = folder.newFolder("conf").toPath();

        // a stand-in for a tool-using agent: it reports where it was pointed and answers with the reference
        final Path agent = CommandDriverTest.script(conf,
                "cat > /dev/null\necho \"endpoint=$FLUENCY_GREMLIN_ENDPOINT\" >&2\necho 'g.V(vid4).bothE().otherV()'\n");
        Files.write(conf.resolve("matrix.yaml"), String.join("\n",
                "corpus: {graphs: [modern, empty]}",
                "differential: {seeds: []}",
                "setups:",
                "  - name: plain",
                "    driver: {command: [" + agent + "]}",
                "  - name: tools",
                "    server: true",
                "    driver: {command: [" + agent + "]}").getBytes(StandardCharsets.UTF_8));

        final Path workDir = folder.newFolder("run").toPath();
        FluencyRunner.run("test", Matrix.load(conf.resolve("matrix.yaml")), Corpus.fromClasspath(), workDir);

        final List<String> rows = Files.readAllLines(workDir.resolve("attempts.jsonl"), StandardCharsets.UTF_8);
        assertThat(rows.stream().filter(r -> r.contains("\"setup\":\"plain\"")).
                noneMatch(r -> r.contains("endpoint=localhost")), is(true));
        assertThat(rows.stream().filter(r -> r.contains("\"setup\":\"tools\"") && r.contains("\"graph\":\"modern\"")).
                allMatch(r -> r.matches(".*endpoint=localhost:\\d+/g\\d+\\b.*")), is(true));
        assertThat(rows.stream().filter(r -> r.contains("\"setup\":\"tools\"") && r.contains("\"graph\":\"empty\"")).
                allMatch(r -> r.matches(".*endpoint=localhost:\\d+/g\\d+\\b.*")), is(true));
    }

    @Test
    public void shouldSummarizeOutcomesPerSetup() {
        final IntentScenario scenario = CorpusTest.find("g_VX4X_bothE_otherV");
        final String summary = FluencyRunner.summary(List.of(
                new ScoredAttempt(new Candidate(scenario, "s#a#1", "x"), Outcome.PASS, null),
                new ScoredAttempt(new Candidate(scenario, "s#a#2", "x"), Outcome.PASS, null).
                        withDifferential(DifferentialOutcome.COINCIDENTAL, "seed 1"),
                new ScoredAttempt(new Candidate(scenario, "s#a#3", "x"), Outcome.PASS, null).
                        withDifferential(DifferentialOutcome.AGREES, null),
                new ScoredAttempt(new Candidate(scenario, "s#b#1", null), Outcome.NO_ANSWER, null)));
        assertThat(summary, is("a: {PASS=3} passes {AGREES=1, COINCIDENTAL=1} verified=1\nb: {NO_ANSWER=1}"));
    }
}
