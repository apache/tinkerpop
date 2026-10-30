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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ReportTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldWriteAReportBesideTheSnapshot() throws Exception {
        final Path runDir = folder.newFolder("run").toPath();
        SnapshotTest.writeRun(runDir);
        Files.write(runDir.resolve("attempts.jsonl"), Arrays.asList(
                SnapshotTest.row("none", "s1", "WRONG_RESULT", null, "g.V().has('age', lt(30))", null),
                SnapshotTest.row("mcp", "s1", "PASS", "AGREES", "g.V()", Map.of("turns", 3),
                        List.of("mcp__gremlin__get_graph_schema", "mcp__gremlin__run_gremlin_query")),
                SnapshotTest.row("none", "s2", "PASS", "AGREES", "g.V()", null),
                SnapshotTest.row("mcp", "s2", "PASS", "COINCIDENTAL", "g.V().limit(1)", null)), StandardCharsets.UTF_8);

        Snapshot.write(runDir);
        final String html = new String(Files.readAllBytes(runDir.resolve("report.html")), StandardCharsets.UTF_8);

        assertThat(html.startsWith("<!doctype html>"), is(true));
        // nothing is loaded from elsewhere, and each scenario name can be copied
        assertThat(html, not(containsString("<script src")));
        assertThat(html, not(containsString("<link")));
        assertThat(html, containsString("<button class=\"copy\" type=\"button\" data-copy=\"s1\""));
        assertThat(html, not(containsString("{{")));
        // the comparison of the tool context against none for the same model leads the report
        assertThat(html, containsString("<td>mcp</td><td>gremlin-mcp</td><td>none</td>"));
        // the traversals written are escaped and shown with their outcomes
        assertThat(html, containsString("<pre>g.V().has('age', lt(30))</pre>"));
        assertThat(html, containsString("<span class=\"tag COINCIDENTAL\">coincidental</span>"));
        assertThat(html, containsString("<pre>g.V().limit(1)</pre>"));
        // tool use is summarized per setup and shown beside the traversal it led to, without the MCP server prefix
        assertThat(html, containsString("<h2>Tool use</h2>"));
        assertThat(html, containsString("<code>get_graph_schema</code>"));
        assertThat(html, containsString("get_graph_schema → run_gremlin_query ×1"));
    }

    @Test
    public void shouldWarnWhenTheReferenceControlFails() {
        final Map<String, Object> snapshot = Map.of("runId", "r", "setups", Arrays.asList(Map.of(
                "name", "reference", "driver", "reference", "attempts", 2, "verified", 1, "passRate", 0.5,
                "verifiedPassRate", 0.5)));
        assertThat(Report.render(snapshot), containsString("should not be trusted"));
    }

    @Test
    public void shouldEscapeMarkup() {
        assertThat(Report.escape("<a href=\"x\">&</a>"), is("&lt;a href=&quot;x&quot;&gt;&amp;&lt;/a&gt;"));
    }
}
