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
package org.apache.tinkerpop.gremlin.docs;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.Attributes;
import org.asciidoctor.Options;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link GremlinTreeprocessor} that verify block processing logic
 * without requiring a real Gremlin Console subprocess.
 */
public class GremlinTreeprocessorTest {

    @Test
    public void shouldDetectSugarSyntax() {
        assertThat(GremlinTreeprocessor.usesSugarSyntax("g.V"), is(true));
        assertThat(GremlinTreeprocessor.usesSugarSyntax("g.V.name"), is(true));
        assertThat(GremlinTreeprocessor.usesSugarSyntax("g.V.outE.weight"), is(true));
        assertThat(GremlinTreeprocessor.usesSugarSyntax("g.V[0..2]"), is(true));
        assertThat(GremlinTreeprocessor.usesSugarSyntax("g.V[0..<2]"), is(true));
        assertThat(GremlinTreeprocessor.usesSugarSyntax("g.E"), is(true));
        // Normal (non-sugar) statements
        assertThat(GremlinTreeprocessor.usesSugarSyntax("g.V()"), is(false));
        assertThat(GremlinTreeprocessor.usesSugarSyntax("g.V().values('name')"), is(false));
        assertThat(GremlinTreeprocessor.usesSugarSyntax("g.V(1).out()"), is(false));
        assertThat(GremlinTreeprocessor.usesSugarSyntax("g.E().count()"), is(false));
        assertThat(GremlinTreeprocessor.usesSugarSyntax("graph = TinkerFactory.createModern()"), is(false));
    }

    @Test
    public void shouldExecuteModernGraphBlock() {
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            assertThat(result, is(notNullValue()));
            assertThat(processor.getGremlinBlockCount(), is(1));
            assertThat(executor.statements.contains("graph = TinkerFactory.createModern()"), is(true));
            assertThat(executor.statements.contains("g = graph.traversal()"), is(true));
            assertThat(executor.statements.contains("g.V(1)"), is(true));
        }
    }

    @Test
    public void shouldUseTinkerGraphForBareBlock() {
        final RecordingExecutor executor = new RecordingExecutor("==>2");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy]\n----\n1+1\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            assertThat(result, is(notNullValue()));
            assertThat(executor.statements.contains("graph = TinkerGraph.open(conf)"), is(true));
            assertThat(executor.statements.contains("g = graph.traversal()"), is(true));
        }
    }

    @Test
    public void shouldReuseGraphStateForExisting() {
        final RecordingExecutor executor = new RecordingExecutor("==>result");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n" +
                    "[gremlin-groovy,modern]\n----\ng.V(1)\n----\n\n" +
                    "[gremlin-groovy,existing]\n----\ng.E()\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            assertThat(result, is(notNullValue()));
            assertThat(processor.getGremlinBlockCount(), is(2));
            // Should only have one graph init (for modern), not for existing
            final long initCount = executor.statements.stream()
                    .filter(s -> s.startsWith("graph = ")).count();
            assertThat(initCount, is(1L));
        }
    }

    @Test
    public void shouldReInitGraphForEachBlock() {
        // Each non-'existing' block must re-init graph and g, because a prior
        // block may have mutated graph or reassigned g (e.g. withComputer()).
        final RecordingExecutor executor = new RecordingExecutor("==>result");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n" +
                    "[gremlin-groovy,modern]\n----\ng.V(1)\n----\n\n" +
                    "[gremlin-groovy,modern]\n----\ng.V(2)\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            assertThat(result, is(notNullValue()));
            assertThat(processor.getGremlinBlockCount(), is(2));
            final long modernInitCount = executor.statements.stream()
                    .filter(s -> s.equals("graph = TinkerFactory.createModern()")).count();
            assertThat(modernInitCount, is(2L));
        }
    }

    @Test
    public void shouldReInitWhenGraphChanges() {
        final RecordingExecutor executor = new RecordingExecutor("==>result");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n" +
                    "[gremlin-groovy,modern]\n----\ng.V(1)\n----\n\n" +
                    "[gremlin-groovy,classic]\n----\ng.V(1)\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            assertThat(result, is(notNullValue()));
            assertThat(executor.statements.contains("graph = TinkerFactory.createModern()"), is(true));
            assertThat(executor.statements.contains("graph = TinkerFactory.createClassic()"), is(true));
        }
    }

    @Test
    public void shouldFormatOutputWithPromptAndTabs() {
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            assertThat(result, containsString("gremlin"));
        }
    }

    @Test
    public void shouldFailBuildWhenStatementErrors() {
        // A statement that raises a Gremlin error (signalled by the console via a
        // GremlinExecutionException) must fail the docs build rather than render as a
        // silently-empty block.
        final String erroringStatement = "g.V().fail('we failed!')";
        final GremlinTreeprocessor.StatementExecutor erroringExecutor = statement -> {
            if (statement.equals(erroringStatement)) {
                throw new GremlinConsole.GremlinExecutionException(statement, "fail() Step Triggered");
            }
            return "==>ok";
        };
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(erroringExecutor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy,modern]\n----\n" + erroringStatement + "\n----\n";
            asciidoctor.convert(input, Options.builder().build());
            throw new AssertionError("Expected build to fail with GremlinExecutionException");
        } catch (final GremlinConsole.GremlinExecutionException e) {
            assertThat(e.getMessage(), containsString(erroringStatement));
        }
    }

    @Test
    public void shouldNotFailBuildForEmptyResultBlock() {
        // A block that legitimately produces no ==> result (e.g. a void iterate() or an empty
        // service list) is NOT an error and must build successfully.
        final RecordingExecutor executor = new RecordingExecutor("");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy,modern]\n----\ng.V().iterate()\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            assertThat(result, is(notNullValue()));
            assertThat(processor.getGremlinBlockCount(), is(1));
        }
    }

    @Test
    public void shouldHandleMultiLineCode() {
        final List<String> responses = new ArrayList<>();
        responses.add("==>v[1]\n==>v[2]");
        responses.add("==>2");
        final RecordingExecutor executor = new RecordingExecutor(responses);
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy,modern]\n----\ng.V()\ng.V().count()\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            assertThat(result, is(notNullValue()));
            assertThat(executor.statements.contains("g.V()"), is(true));
            assertThat(executor.statements.contains("g.V().count()"), is(true));
        }
    }

    @Test
    public void shouldKeepMultiLineClosureAsSingleStatement() {
        // A Groovy closure whose closing line is not indented (e.g. "}; []") must stay grouped
        // with its opening line; splitting it would send an incomplete statement to the console
        // and hang at a continuation prompt.
        final String[] lines = {
                "(1..10).each {",
                "  g.addV(\"product\").property(\"name\",\"product #${it}\").iterate()",
                "}; []",
                "g.V().count()"
        };
        final List<String> statements = GremlinTreeprocessor.buildStatements(lines);
        assertThat(statements.size(), is(2));
        assertThat(statements.get(0), containsString("(1..10).each {"));
        assertThat(statements.get(0), containsString("}; []"));
        assertThat(statements.get(1), is("g.V().count()"));
    }

    @Test
    public void shouldNotCountBracketsInsideStrings() {
        // Brackets inside string literals (including GString interpolation) must not affect
        // statement grouping.
        final String[] lines = {
                "x = '}'",
                "y = \"${a}\"",
                "z = 1"
        };
        final List<String> statements = GremlinTreeprocessor.buildStatements(lines);
        assertThat(statements.size(), is(3));
    }

    @Test
    public void shouldHandleCrewGraph() {
        final RecordingExecutor executor = new RecordingExecutor("==>result");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy,crew]\n----\ng.V()\n----\n";
            asciidoctor.convert(input, Options.builder().build());
            assertThat(executor.statements.contains("graph = TinkerFactory.createTheCrew()"), is(true));
        }
    }

    @Test
    public void shouldHandleGratefulGraph() {
        final RecordingExecutor executor = new RecordingExecutor("==>result");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy,grateful]\n----\ng.V()\n----\n";
            asciidoctor.convert(input, Options.builder().build());
            assertThat(executor.statements.contains("graph = TinkerFactory.createGratefulDead()"), is(true));
        }
    }

    @Test
    public void shouldHandleSinkGraph() {
        final RecordingExecutor executor = new RecordingExecutor("==>result");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy,sink]\n----\ng.V()\n----\n";
            asciidoctor.convert(input, Options.builder().build());
            assertThat(executor.statements.contains("graph = TinkerFactory.createKitchenSink()"), is(true));
        }
    }

    @Test
    public void shouldFormatDryRunWithPromptsOnly() {
        final GremlinTreeprocessor processor = new GremlinTreeprocessor();

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy,modern]\n----\ng.V(1)\ng.E()\n----\n";
            final Options options = Options.builder()
                    .attributes(Attributes.builder().attribute("gremlin-docs-dryrun", "").build())
                    .build();
            final String result = asciidoctor.convert(input, options);
            assertThat(result, containsString("gremlin"));
            assertThat(result, containsString("gremlin"));
            assertThat(processor.getGremlinBlockCount(), is(1));
        }
    }

    @Test
    public void shouldNotExecuteInDryRunMode() {
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            final Options options = Options.builder()
                    .attributes(Attributes.builder().attribute("gremlin-docs-dryrun", "").build())
                    .build();
            asciidoctor.convert(input, options);
            assertThat(executor.statements.isEmpty(), is(true));
        }
    }

    @Test
    public void shouldCountBlocksWithoutConsole() {
        final GremlinTreeprocessor processor = new GremlinTreeprocessor();

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy]\n----\ng.V()\n----\n\n" +
                    "[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            asciidoctor.convert(input, Options.builder().build());
            assertThat(processor.getGremlinBlockCount(), is(2));
        }
    }

    @Test
    public void shouldGenerateTabbedHtmlForGremlinBlock() {
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            assertThat(result, containsString("section class=\"tabs tabs-2\""));
            assertThat(result, containsString("console (groovy)"));
            assertThat(result, containsString("tab-group-1"));
        }
    }

    @Test
    public void shouldConsumeManualTabBlocksAfterGremlinBlock() {
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n" +
                    "[gremlin-groovy,modern]\n----\ng.V(1)\n----\n" +
                    "[source,java]\n----\ng.V(1)\n----\n" +
                    "[source,python]\n----\ng.V(1)\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            assertThat(result, containsString("tabs tabs-4"));
            assertThat(result, containsString("console (groovy)"));
            assertThat(result, containsString("tab-label-3\">java"));
            assertThat(result, containsString("tab-label-4\">python"));
        }
    }

    @Test
    public void shouldNotConsumeUnsupportedLanguageAsManualTab() {
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n" +
                    "[gremlin-groovy,modern]\n----\ng.V(1)\n----\n" +
                    "[source,ruby]\n----\ng.V(1)\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            // Only the console tab, ruby is not consumed
            assertThat(result, containsString("tabs tabs-2"));
        }
    }

    @Test
    public void shouldProcessStandaloneTabGroup() {
        final GremlinTreeprocessor processor = new GremlinTreeprocessor();

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n" +
                    "[source,groovy,tab]\n----\ng.V()\n----\n" +
                    "[source,java,tab]\n----\ng.V()\n----\n" +
                    "[source,python,tab]\n----\ng.V()\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            assertThat(result, containsString("tabs tabs-3"));
            assertThat(result, containsString("tab-label-1\">groovy"));
            assertThat(result, containsString("tab-label-2\">java"));
            assertThat(result, containsString("tab-label-3\">python"));
        }
    }

    @Test
    public void shouldKeepStandaloneTabGroupsSeparateFromGremlinGroups() {
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n" +
                    "[gremlin-groovy,modern]\n----\ng.V(1)\n----\n\n" +
                    "[source,groovy,tab]\n----\ncode1\n----\n" +
                    "[source,java,tab]\n----\ncode2\n----\n";
            final String result = asciidoctor.convert(input, Options.builder().build());
            // Should have two separate tab groups
            assertThat(result, containsString("tab-group-1"));
            assertThat(result, containsString("tab-group-2"));
        }
    }

    @Test
    public void shouldInvokeRestartHandlerWhenPluginExcludeAttributePresent() {
        final List<List<String>> restartCalls = new ArrayList<>();
        final ConsoleRestartHandler handler = restartCalls::add;
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor, handler);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n:gremlin-docs-plugins-exclude: neo4j-gremlin,spark-gremlin\n\n" +
                    "[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            asciidoctor.convert(input, Options.builder().build());
            assertThat(restartCalls.size(), is(1));
            assertThat(restartCalls.get(0).contains("neo4j-gremlin"), is(true));
            assertThat(restartCalls.get(0).contains("spark-gremlin"), is(true));
        }
    }

    @Test
    public void shouldNotInvokeRestartHandlerWhenNoExcludeAttribute() {
        final List<List<String>> restartCalls = new ArrayList<>();
        final ConsoleRestartHandler handler = restartCalls::add;
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor, handler);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input = "= Test\n\n[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            asciidoctor.convert(input, Options.builder().build());
            assertThat(restartCalls.isEmpty(), is(true));
        }
    }

    @Test
    public void shouldNotInvokeRestartHandlerWhenExcludeListUnchanged() {
        final List<List<String>> restartCalls = new ArrayList<>();
        final ConsoleRestartHandler handler = restartCalls::add;
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor, handler);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            // Process same document twice - handler should only be called once
            final String input = "= Test\n:gremlin-docs-plugins-exclude: neo4j-gremlin\n\n" +
                    "[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            asciidoctor.convert(input, Options.builder().build());
            asciidoctor.convert(input, Options.builder().build());
            assertThat(restartCalls.size(), is(1));
        }
    }

    @Test
    public void shouldInvokeRestartHandlerWhenExcludeListChanges() {
        final List<List<String>> restartCalls = new ArrayList<>();
        final ConsoleRestartHandler handler = restartCalls::add;
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor, handler);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            final String input1 = "= Test\n:gremlin-docs-plugins-exclude: neo4j-gremlin\n\n" +
                    "[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            asciidoctor.convert(input1, Options.builder().build());

            final String input2 = "= Test\n:gremlin-docs-plugins-exclude: spark-gremlin\n\n" +
                    "[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            asciidoctor.convert(input2, Options.builder().build());
            assertThat(restartCalls.size(), is(2));
            assertThat(restartCalls.get(0).contains("neo4j-gremlin"), is(true));
            assertThat(restartCalls.get(1).contains("spark-gremlin"), is(true));
        }
    }

    @Test
    public void shouldInvokeRestartHandlerForSectionLevelExcludeWithinOneDocument() {
        final List<List<String>> restartCalls = new ArrayList<>();
        final ConsoleRestartHandler handler = restartCalls::add;
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor, handler);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            // Single document with per-section exclusions changing mid-document, as in the reference book.
            final String input = "= Book\n\n"
                    + "== Neo4j\n[gremlin-groovy,modern]\n----\ng.V(1)\n----\n\n"
                    + "[gremlin-docs-plugins-exclude=\"neo4j-gremlin\"]\n"
                    + "== Spark\n[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            asciidoctor.convert(input, Options.builder().build());
            assertThat(restartCalls.size(), is(1));
            assertThat(restartCalls.get(0).contains("neo4j-gremlin"), is(true));
        }
    }

    @Test
    public void shouldLatchSectionExclusionUntilChanged() {
        final List<List<String>> restartCalls = new ArrayList<>();
        final ConsoleRestartHandler handler = restartCalls::add;
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor, handler);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            // Two consecutive excluding sections sharing the same set => one restart;
            // a later section with no attribute inherits (no extra restart).
            final String input = "= Book\n\n"
                    + "[gremlin-docs-plugins-exclude=\"neo4j-gremlin\"]\n"
                    + "== Hadoop\n[gremlin-groovy,modern]\n----\ng.V(1)\n----\n\n"
                    + "[gremlin-docs-plugins-exclude=\"neo4j-gremlin\"]\n"
                    + "== Spark\n[gremlin-groovy,modern]\n----\ng.V(1)\n----\n\n"
                    + "== Compilers\n[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            asciidoctor.convert(input, Options.builder().build());
            assertThat(restartCalls.size(), is(1));
        }
    }

    @Test
    public void shouldParseExcludeListWithWhitespace() {
        final List<String> result = GremlinTreeprocessor.parseExcludeList(" neo4j-gremlin , spark-gremlin , hadoop-gremlin ");
        assertThat(result.size(), is(3));
        assertThat(result.contains("neo4j-gremlin"), is(true));
        assertThat(result.contains("spark-gremlin"), is(true));
        assertThat(result.contains("hadoop-gremlin"), is(true));
    }

    @Test
    public void shouldParseEmptyExcludeList() {
        assertThat(GremlinTreeprocessor.parseExcludeList("").isEmpty(), is(true));
        assertThat(GremlinTreeprocessor.parseExcludeList("  ").isEmpty(), is(true));
        assertThat(GremlinTreeprocessor.parseExcludeList(null).isEmpty(), is(true));
    }

    @Test
    public void shouldInvokeRestartHandlerWithEmptyListWhenAttributeRemoved() {
        final List<List<String>> restartCalls = new ArrayList<>();
        final ConsoleRestartHandler handler = restartCalls::add;
        final RecordingExecutor executor = new RecordingExecutor("==>v[1]");
        final GremlinTreeprocessor processor = new GremlinTreeprocessor(executor, handler);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(processor);
            // First doc has exclusions
            final String input1 = "= Test\n:gremlin-docs-plugins-exclude: neo4j-gremlin\n\n" +
                    "[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            asciidoctor.convert(input1, Options.builder().build());

            // Second doc has no exclusions
            final String input2 = "= Test\n\n[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";
            asciidoctor.convert(input2, Options.builder().build());
            assertThat(restartCalls.size(), is(2));
            assertThat(restartCalls.get(1).isEmpty(), is(true));
        }
    }

    /**
     * A recording executor that captures all executed statements and returns canned responses.
     */
    private static class RecordingExecutor implements GremlinTreeprocessor.StatementExecutor {
        final List<String> statements = new ArrayList<>();
        private final List<String> responses;
        private int responseIndex = 0;

        RecordingExecutor(final String response) {
            this.responses = new ArrayList<>();
            if (response != null) {
                this.responses.add(response);
            }
        }

        RecordingExecutor(final List<String> responses) {
            this.responses = responses;
        }

        @Override
        public String execute(final String statement) {
            statements.add(statement);
            if (responses.isEmpty()) {
                return "";
            }
            final String response = responses.get(Math.min(responseIndex, responses.size() - 1));
            responseIndex++;
            return response;
        }
    }
}
