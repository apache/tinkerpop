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

import org.apache.tinkerpop.gremlin.language.translator.Translator;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class GremlinExecutorTest {

    @Test
    public void shouldExecuteSimpleTraversal() throws Exception {
        try (final GremlinExecutor executor = new GremlinExecutor()) {
            executor.initGraph("modern");
            final String output = executor.execute(Arrays.asList("g.V().count()"));
            assertTrue(output.contains("gremlin> g.V().count()"));
            assertTrue(output.contains("==>6"));
        }
    }

    @Test
    public void shouldMaintainStateBetweenExecutions() throws Exception {
        try (final GremlinExecutor executor = new GremlinExecutor()) {
            executor.initGraph("modern");
            executor.execute(Arrays.asList("x = g.V().has('name','marko').next()"));

            // "existing" should reuse the graph and bindings
            executor.initGraph("existing");
            final String output = executor.execute(Arrays.asList("x.value('name')"));
            assertTrue(output.contains("==>marko"));
        }
    }

    @Test
    public void shouldExecuteMultipleLines() throws Exception {
        try (final GremlinExecutor executor = new GremlinExecutor()) {
            executor.initGraph("modern");
            final String output = executor.execute(Arrays.asList(
                    "g.V().has('name','marko').values('name')",
                    "g.V().has('name','marko').out('knows').values('name')"
            ));
            assertTrue(output.contains("==>marko"));
            assertTrue(output.contains("==>josh"));
            assertTrue(output.contains("==>vadas"));
        }
    }

    @Test
    public void shouldExtractTranslatableLines() {
        final List<String> lines = Arrays.asList(
                "g.V().has('name','marko'). <1>",
                "  out('knows').values('name') <2>",
                "// this is a comment",
                "g.V().count()"
        );
        final List<String> result = GremlinExecutor.extractTranslatableLines(lines);
        assertEquals(2, result.size());
        assertEquals("g.V().has('name','marko').\nout('knows').values('name')", result.get(0));
        assertEquals("g.V().count()", result.get(1));
    }

    @Test
    public void shouldTranslateToVariants() {
        final Map<Translator, String> translations = VariantTranslator.translateStatement(
                "g.V().has('name','marko').out('knows').values('name')");

        assertFalse(translations.isEmpty());
        assertTrue(translations.containsKey(Translator.PYTHON));
        assertTrue(translations.containsKey(Translator.JAVA));
        assertTrue(translations.containsKey(Translator.JAVASCRIPT));
        assertTrue(translations.containsKey(Translator.DOTNET));
        assertTrue(translations.containsKey(Translator.GO));

        // python should use snake_case
        assertTrue(translations.get(Translator.PYTHON).contains("has("));
        assertTrue(translations.get(Translator.PYTHON).contains("out("));
    }

    @Test
    public void shouldTranslateBlock() {
        final List<String> statements = Arrays.asList(
                "g.V().has('name','marko').out('knows').values('name')",
                "g.V().count()"
        );
        final Map<Translator, String> translations = VariantTranslator.translateBlock(statements);

        assertFalse(translations.isEmpty());
        // each translation should contain both statements
        for (final String code : translations.values()) {
            assertTrue(code.contains("\n"));
        }
    }

    @Test
    public void shouldInitEmptyGraph() throws Exception {
        try (final GremlinExecutor executor = new GremlinExecutor()) {
            executor.initGraph(null);
            final String output = executor.execute(Arrays.asList("g.V().count()"));
            assertTrue(output.contains("==>0"));
        }
    }

    @Test
    public void shouldInitEmptyStringGraph() throws Exception {
        try (final GremlinExecutor executor = new GremlinExecutor()) {
            executor.initGraph("");
            final String output = executor.execute(Arrays.asList("g.V().count()"));
            assertTrue(output.contains("==>0"));
        }
    }

    @Test
    public void shouldSkipUntranslatableStatements() {
        // lambdas can't be translated
        final Map<Translator, String> translations = VariantTranslator.translateStatement(
                "g.V().filter{it.get().label() == 'person'}");
        // should either be empty or have partial results — not throw
        assertNotNull(translations);
    }
}
