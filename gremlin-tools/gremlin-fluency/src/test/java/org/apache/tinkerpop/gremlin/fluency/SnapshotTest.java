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

import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

public class SnapshotTest {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldSummarizeSetupsAndCompareContextsWithinAModel() throws Exception {
        final Path runDir = folder.newFolder("run").toPath();
        writeRun(runDir);
        final List<String> rows = new ArrayList<>();
        // the baseline passes s1 once in two attempts and fails s2; the tool setup passes both, once coincidentally
        rows.add(row("none", "s1", "PASS", "AGREES", "g.V()", Map.of("input_tokens", 100, "output_tokens", 10, "turns", 1)));
        rows.add(row("none", "s1", "WRONG_RESULT", null, "g.E()", Map.of("input_tokens", 300, "output_tokens", 30, "turns", 1)));
        rows.add(row("none", "s2", "PARSE_FAILURE", null, "g.V(", null));
        rows.add(row("none", "s2", "PARSE_FAILURE", null, "g.V(", null));
        rows.add(row("mcp", "s1", "PASS", "AGREES", "g.V()", null));
        rows.add(row("mcp", "s1", "PASS", "AGREES", "g.V()", null));
        rows.add(row("mcp", "s2", "PASS", "COINCIDENTAL", "g.V().limit(1)", null));
        rows.add(row("mcp", "s2", "PASS", "AGREES", "g.V().has('x')", null));
        rows.add(row("reference", "s1", "PASS", "AGREES", "g.V()", null));
        Files.write(runDir.resolve("attempts.jsonl"), rows, StandardCharsets.UTF_8);

        final Map<String, Object> snapshot = Snapshot.build(runDir);
        assertThat(snapshot.get("corpusHash"), is("c0ffee"));

        final List<Map<String, Object>> setups = list(snapshot, "setups");
        final Map<String, Object> none = setups.get(1);
        assertThat(none.get("passRate"), is(0.25));
        assertThat(none.get("outcomes"), is(Map.of("PASS", 1L, "WRONG_RESULT", 1L, "PARSE_FAILURE", 2L)));
        assertThat(map(none, "usage").get("attemptsReporting"), is(2));
        assertThat(map(none, "usage").get("totals"), is(Map.of("input_tokens", 400L, "output_tokens", 40L, "turns", 2L)));
        assertThat(map(none, "usage").get("perAttempt"), is(Map.of("input_tokens", 200.0, "output_tokens", 20.0, "turns", 1.0)));

        final Map<String, Object> mcp = setups.get(2);
        assertThat(mcp.get("passRate"), is(1.0));
        assertThat(mcp.get("verifiedPassRate"), is(0.75));
        assertThat(mcp.get("differential"), is(Map.of("AGREES", 3L, "COINCIDENTAL", 1L)));
        assertThat(mcp.get("usage"), is(nullValue()));
        assertThat(map(mcp, "byGraph").keySet(), contains("modern"));

        // the reference control has no model, so only the tool setup is compared, against its own model's none
        final List<Map<String, Object>> comparisons = list(snapshot, "comparisons");
        assertThat(comparisons.size(), is(1));
        final Map<String, Object> comparison = comparisons.get(0);
        assertThat(comparison.get("baseline"), is("none"));
        assertThat(comparison.get("setup"), is("mcp"));
        assertThat(comparison.get("scenarios"), is(2));
        assertThat(map(comparison, "raw").get("difference"), is(0.75));
        assertThat(map(comparison, "verified").get("difference"), is(0.5));
        @SuppressWarnings("unchecked")
        final List<Double> interval = (List<Double>) map(comparison, "verified").get("interval");
        assertThat(interval.get(0), lessThan(0.5));
        assertThat(interval.get(1), greaterThan(0.5));

        final Map<String, Object> s1 = list(snapshot, "scenarios").get(0);
        assertThat(s1.get("hash"), is("aaa"));
        final List<Map<String, Object>> traversals = list(map(map(s1, "setups"), "none"), "traversals");
        assertThat(traversals.size(), is(2));
        final List<Map<String, Object>> mcpTraversals = list(map(map(s1, "setups"), "mcp"), "traversals");
        assertThat(mcpTraversals.size(), is(1));
        assertThat(mcpTraversals.get(0).get("count"), is(2L));

        // rebuilding gives the same snapshot, intervals included
        assertThat(mapper.writeValueAsString(Snapshot.build(runDir)), is(mapper.writeValueAsString(snapshot)));
    }

    @Test
    public void shouldSummarizeToolCalls() throws Exception {
        final Path runDir = folder.newFolder("run").toPath();
        writeRun(runDir);
        final List<String> schemaThenQuery = List.of("mcp__gremlin__get_graph_schema", "mcp__gremlin__run_gremlin_query",
                "mcp__gremlin__run_gremlin_query");
        Files.write(runDir.resolve("attempts.jsonl"), Arrays.asList(
                row("mcp", "s1", "PASS", "AGREES", "g.V()", null, schemaThenQuery),
                row("mcp", "s1", "PASS", "AGREES", "g.V()", null, List.of("mcp__gremlin__get_graph_schema")),
                row("mcp", "s2", "WRONG_RESULT", null, "g.E()", null, List.of()),
                row("mcp", "s2", "PASS", "COINCIDENTAL", "g.V().limit(1)", null, List.of()),
                // a driver that did not report its calls is left out rather than counted as calling nothing
                row("none", "s1", "PASS", "AGREES", "g.V()", null, null)), StandardCharsets.UTF_8);

        final Map<String, Object> snapshot = Snapshot.build(runDir);
        assertThat(list(snapshot, "setups").get(1).get("tools"), is(nullValue()));

        final Map<String, Object> tools = map(list(snapshot, "setups").get(2), "tools");
        assertThat(tools.get("attemptsReporting"), is(4));
        assertThat(tools.get("callsPerAttempt"), is(1.0));
        assertThat(map(tools, "byTool"), is(Map.of(
                "mcp__gremlin__get_graph_schema", Map.of("calls", 2L, "attempts", 2L, "verified", 2L),
                "mcp__gremlin__run_gremlin_query", Map.of("calls", 2L, "attempts", 1L, "verified", 1L))));
        assertThat(map(tools, "withoutTools"), is(Map.of("attempts", 2L, "verified", 0L)));

        // each distinct traversal carries the distinct sequences of calls behind it
        final List<Map<String, Object>> traversals = list(map(map(list(snapshot, "scenarios").get(0), "setups"), "mcp"),
                "traversals");
        assertThat(traversals.get(0).get("tools"), is(List.of(
                Map.of("calls", schemaThenQuery, "count", 1L),
                Map.of("calls", List.of("mcp__gremlin__get_graph_schema"), "count", 1L))));
    }

    @Test
    public void shouldSummarizeARunThatStoppedBeforeAnyAttempt() throws Exception {
        final Path runDir = folder.newFolder("run").toPath();
        writeRun(runDir);
        final Map<String, Object> snapshot = Snapshot.write(runDir);
        assertThat(list(snapshot, "setups").get(0).get("attempts"), is(0));
        assertThat(list(snapshot, "setups").get(0).get("passRate"), is(nullValue()));
        assertThat(list(snapshot, "comparisons").isEmpty(), is(true));
        assertThat(Files.exists(runDir.resolve("snapshot.json")), is(true));
    }

    static void writeRun(final Path runDir) throws Exception {
        final Map<String, Object> run = new LinkedHashMap<>();
        run.put("runId", "test");
        run.put("corpusHash", "c0ffee");
        run.put("scenarios", Arrays.asList(
                Map.of("name", "s1", "feature", "map/A.feature", "graph", "modern", "hash", "aaa"),
                Map.of("name", "s2", "feature", "map/B.feature", "graph", "modern", "hash", "bbb")));
        run.put("setups", Arrays.asList(
                Map.of("name", "reference"),
                Map.of("name", "none", "model", "m", "harness", "h", "context", List.of("none")),
                Map.of("name", "mcp", "model", "m", "harness", "h", "context", List.of("gremlin-mcp"))));
        mapper.writeValue(runDir.resolve("run.json").toFile(), run);
    }

    static String row(final String setup, final String scenario, final String outcome, final String differential,
                              final String traversal, final Map<String, Integer> usage) throws Exception {
        return row(setup, scenario, outcome, differential, traversal, usage, null);
    }

    static String row(final String setup, final String scenario, final String outcome, final String differential,
                      final String traversal, final Map<String, Integer> usage, final List<String> tools) throws Exception {
        final Map<String, Object> row = new LinkedHashMap<>();
        row.put("setup", setup);
        row.put("scenario", scenario);
        row.put("feature", "map/A.feature");
        row.put("graph", "modern");
        row.put("outcome", outcome);
        row.put("differential", differential);
        row.put("traversal", traversal);
        row.put("usage", usage);
        row.put("tools", tools);
        return mapper.writeValueAsString(row);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> list(final Map<String, Object> map, final String key) {
        return (List<Map<String, Object>>) map.get(key);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(final Map<String, Object> map, final String key) {
        return (Map<String, Object>) map.get(key);
    }
}
