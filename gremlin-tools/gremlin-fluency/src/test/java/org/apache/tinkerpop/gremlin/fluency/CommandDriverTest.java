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

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class CommandDriverTest {

    @ClassRule
    public static final TemporaryFolder scripts = new TemporaryFolder();

    private static final IntentScenario scenario = CorpusTest.find("g_VX4X_bothE_otherV");

    @Test
    public void shouldPassPromptAndEnvironmentAndCleanTheAnswer() throws Exception {
        // echoes back what it was given so the test can see it: that no graph is named, the attempt, whether the
        // working directory is empty and whether the prompt arrived
        final Path script = script("prompt=$(cat)\n" +
                "echo 'fluency-version: test 1.0' >&2\n" +
                "printf '```groovy\\n%s %s %s %s %s\\n```\\n' \"${FLUENCY_GRAPH:-unnamed}\" \"$FLUENCY_ATTEMPT\" \"$(ls -A | wc -l | tr -d ' ')\" \"$EXTRA\" \"$(echo \"$prompt\" | grep -c 'Write one Gremlin traversal')\"\n");
        final CommandDriver driver = new CommandDriver(Collections.singletonList(script.toString()),
                Collections.singletonMap("EXTRA", "x"), Duration.ofSeconds(10));

        final Answer answer = driver.answer(scenario, Prompt.render(scenario), 7);
        assertThat(answer.getTraversal(), is("unnamed 7 0 x 1"));
        assertThat(driver.getVersion(), is("test 1.0"));
    }

    @Test
    public void shouldReadReportedUsage() throws Exception {
        final Answer answer = driver(script("cat > /dev/null\n" +
                "echo 'fluency-usage: input_tokens=1200 output_tokens=40 turns=2' >&2\necho 'g.V()'\n"),
                Duration.ofSeconds(10)).answer(scenario, "p", 1);
        assertThat(answer.getUsage(), is(Map.of("input_tokens", 1200L, "output_tokens", 40L, "turns", 2L)));
    }

    @Test
    public void shouldReadReportedTools() throws Exception {
        final Answer answer = driver(script("cat > /dev/null\n" +
                "echo 'fluency-tools: get_graph_schema run_gremlin_query' >&2\necho 'g.V()'\n"),
                Duration.ofSeconds(10)).answer(scenario, "p", 1);
        assertThat(answer.getTools(), is(Arrays.asList("get_graph_schema", "run_gremlin_query")));
    }

    @Test
    public void shouldTellNoToolsCalledFromToolsNotReported() throws Exception {
        final Answer none = driver(script("cat > /dev/null\necho 'fluency-tools: ' >&2\necho 'g.V()'\n"),
                Duration.ofSeconds(10)).answer(scenario, "p", 1);
        assertThat(none.getTools(), is(Collections.emptyList()));
        final Answer unreported = driver(script("cat > /dev/null\necho 'g.V()'\n"), Duration.ofSeconds(10)).
                answer(scenario, "p", 1);
        assertThat(unreported.getTools(), is(nullValue()));
    }

    @Test
    public void shouldLeaveUsageEmptyWhenNotReported() throws Exception {
        final Answer answer = driver(script("cat > /dev/null\necho 'g.V()'\n"), Duration.ofSeconds(10)).
                answer(scenario, "p", 1);
        assertThat(answer.getUsage().isEmpty(), is(true));
    }

    @Test
    public void shouldGiveNoAnswerOnNonZeroExit() throws Exception {
        final Answer answer = driver(script("cat > /dev/null\necho broken >&2\nexit 3\n"), Duration.ofSeconds(10)).
                answer(scenario, "p", 1);
        assertThat(answer.getTraversal(), is(nullValue()));
        assertThat(answer.getDetail(), containsString("exited with 3: broken"));
    }

    @Test
    public void shouldGiveNoAnswerOnTimeout() throws Exception {
        final Answer answer = driver(script("sleep 10\n"), Duration.ofMillis(500)).answer(scenario, "p", 1);
        assertThat(answer.getTraversal(), is(nullValue()));
        assertThat(answer.getDetail(), containsString("timed out"));
    }

    @Test
    public void shouldGiveNoAnswerForEmptyOutput() throws Exception {
        final Answer answer = driver(script("cat > /dev/null\n"), Duration.ofSeconds(10)).answer(scenario, "p", 1);
        assertThat(answer.getTraversal(), is(nullValue()));
    }

    @Test
    public void shouldStripOnePairOfSurroundingBackticks() {
        assertThat(CommandDriver.clean("`g.V().hasLabel(\"person\")`\n"), is("g.V().hasLabel(\"person\")"));
        assertThat(CommandDriver.clean("``g.V()``"), is("``g.V()``"));
        assertThat(CommandDriver.clean("`g.V()` and `g.E()`"), is("`g.V()` and `g.E()`"));
        assertThat(CommandDriver.clean("```\n`g.V()`\n```"), is("`g.V()`"));
    }

    @Test
    public void shouldStripOnlyOneSurroundingFence() {
        assertThat(CommandDriver.clean("  g.V()  \n"), is("g.V()"));
        assertThat(CommandDriver.clean("```\ng.V()\n```"), is("g.V()"));
        assertThat(CommandDriver.clean("```gremlin\ng.V().\n  out()\n```"), is("g.V().\n  out()"));
        assertThat(CommandDriver.clean("Here it is: g.V()"), is("Here it is: g.V()"));
        assertThat(CommandDriver.clean("text\n```\ng.V()\n```"), is("text\n```\ng.V()\n```"));
    }

    private static CommandDriver driver(final Path script, final Duration timeout) {
        return new CommandDriver(Collections.singletonList(script.toString()), Collections.emptyMap(), timeout);
    }

    private static Path script(final String body) throws IOException {
        return script(scripts.getRoot().toPath(), body);
    }

    static Path script(final Path directory, final String body) throws IOException {
        final Path script = Files.createTempFile(directory, "driver", ".sh");
        Files.write(script, ("#!/usr/bin/env bash\n" + body).getBytes(StandardCharsets.UTF_8));
        script.toFile().setExecutable(true);
        return script;
    }
}
