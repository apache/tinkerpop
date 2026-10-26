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
import org.yaml.snakeyaml.Yaml;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MatrixTest {
    private static final Path base = Paths.get("/work/conf");

    @Test
    public void shouldReadMatrix() {
        final Matrix matrix = Matrix.fromMap(yaml(
                "corpus: {graphs: [modern]}\n" +
                "attempts: 2\n" +
                "parallelism: 4\n" +
                "timeouts: {driver: 2m, traversal: 500ms}\n" +
                "setups:\n" +
                "  - name: sonnet-none\n" +
                "    model: claude-sonnet-5-5\n" +
                "    harness: claude-code\n" +
                "    context: [none]\n" +
                "    driver: {command: [../bin/claude-code.sh, --mcp], env: {MODEL: m}}\n" +
                "  - name: claude-on-path\n" +
                "    driver: {command: [claude-wrapper]}\n" +
                "  - name: replay\n" +
                "    driver: {builtin: replay, file: answers.yaml}\n"), base);

        assertThat(matrix.getGraphs(), is(Collections.singletonList("modern")));
        assertThat(matrix.getAttempts(), is(2));
        assertThat(matrix.getParallelism(), is(4));
        assertThat(matrix.getDriverTimeout(), is(Duration.ofMinutes(2)));
        assertThat(matrix.getTraversalTimeout(), is(Duration.ofMillis(500)));
        assertThat(matrix.getSetups().get(0).getCommand(), is(Arrays.asList("/work/bin/claude-code.sh", "--mcp")));
        assertThat(matrix.getSetups().get(0).getContext(), is(Collections.singletonList("none")));
        assertThat(matrix.getSetups().get(1).getCommand(), is(Collections.singletonList("claude-wrapper")));
        assertThat(matrix.getSetups().get(2).getBuiltin(), is("replay"));
    }

    @Test
    public void shouldDefaultAttemptsAndTimeouts() {
        final Matrix matrix = Matrix.fromMap(yaml("setups: [{name: r, driver: {builtin: reference}}]"), base);
        assertThat(matrix.getAttempts(), is(1));
        assertThat(matrix.getParallelism(), is(1));
        assertThat(matrix.getDriverTimeout(), is(Duration.ofSeconds(120)));
        assertThat(matrix.getTraversalTimeout(), is(Duration.ofSeconds(30)));
        assertThat(matrix.getGraphs().isEmpty(), is(true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectParallelismBelowOne() {
        Matrix.fromMap(yaml("parallelism: 0\nsetups: [{name: r, driver: {builtin: reference}}]"), base);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectDuplicateSetupNames() {
        Matrix.fromMap(yaml("setups: [{name: r, driver: {builtin: reference}}, {name: r, driver: {builtin: reference}}]"), base);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectSetupWithoutDriver() {
        Matrix.fromMap(yaml("setups: [{name: r}]"), base);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectUnsafeSetupName() {
        Matrix.fromMap(yaml("setups: [{name: ../r, driver: {builtin: reference}}]"), base);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectBadDuration() {
        Matrix.duration("30 seconds");
    }

    private static Map<String, Object> yaml(final String text) {
        return new Yaml().load(text);
    }
}
