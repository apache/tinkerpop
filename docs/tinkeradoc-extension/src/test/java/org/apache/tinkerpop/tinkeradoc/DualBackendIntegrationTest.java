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
package org.apache.tinkerpop.tinkeradoc;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.Options;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * End-to-end test of the dual-backend pipeline: one document, rendered to HTML and to Markdown from
 * a SINGLE Gremlin execution via the shared {@link GremlinExecutionCache}. Verifies that the
 * executed {@code ==>} output appears identically in both backends (parity) and that each backend's
 * tab rendering is correct.
 */
public class DualBackendIntegrationTest {

    private static final class RecordingExecutor implements GremlinTreeprocessor.StatementExecutor {
        final List<String> statements = new ArrayList<>();

        @Override
        public String execute(final String statement) {
            statements.add(statement);
            // The real Gremlin console echoes the submitted statement on the first line and puts
            // ==> results on subsequent lines; the treeprocessor drops that echo line. Mimic that
            // shape so the executed result survives into the rendered console transcript.
            if (statement.equals("g.V(1)")) return statement + "\n==>v[1]";
            return statement + "\n==>ok";
        }
    }

    private static final String DOC =
            "= Guide\n\n== Basics\n\nSome intro text.\n\n"
            + "[gremlin-groovy,modern]\n----\ng.V(1)\n----\n";

    @Test
    public void oneExecutionFeedsBothBackends() {
        final RecordingExecutor executor = new RecordingExecutor();
        final GremlinExecutionCache cache = new GremlinExecutionCache();

        final String html;
        final String markdown;

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.javaConverterRegistry().register(MarkdownConverter.class);

            // Pass 1: HTML backend executes Gremlin and populates the cache.
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(new GremlinTreeprocessor(executor, null, cache));
            html = asciidoctor.convert(DOC, Options.builder().backend("html5").build());
            final int executedAfterHtml = executor.statements.size();

            // Pass 2: Markdown backend must reuse the cache and execute nothing more.
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(new GremlinTreeprocessor(executor, null, cache));
            markdown = asciidoctor.convert(DOC, Options.builder().backend("tpmarkdown").build());
            final int executedAfterMd = executor.statements.size();

            assertThat("Markdown pass must not re-execute Gremlin", executedAfterMd, is(executedAfterHtml));
        }

        // HTML backend: tab widget with the executed result. CodeRay highlights the numeric index,
        // so the result renders as v[<span class="integer">1</span>] rather than literal "v[1]".
        assertThat(html, containsString("section class=\"tabs"));
        assertThat(html, containsString("==&gt;v["));
        assertThat(html, containsString("<span class=\"integer\">1</span>"));

        // Markdown backend: heading, prose, and a fenced console block with the executed result
        // preserved verbatim (no highlighting).
        assertThat(markdown, containsString("## Basics"));
        assertThat(markdown, containsString("Some intro text."));
        assertThat(markdown, containsString("**console (groovy)**"));
        assertThat(markdown, containsString("```text"));
        assertThat(markdown, containsString("gremlin> g.V(1)"));
        assertThat(markdown, containsString("==>v[1]"));

        // Parity: the executed ==> output appears in BOTH backends (HTML highlights the integer,
        // Markdown keeps it plain, but the same executed value is present in each).
        assertThat("executed output present in HTML", html.contains("==&gt;v["), is(true));
        assertThat("executed output present in Markdown", markdown.contains("==>v[1]"), is(true));
    }

    @Test
    public void markdownDoesNotContainHtmlTabWidget() {
        final RecordingExecutor executor = new RecordingExecutor();
        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.javaConverterRegistry().register(MarkdownConverter.class);
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(
                    new GremlinTreeprocessor(executor, null, new GremlinExecutionCache()));
            final String markdown = asciidoctor.convert(DOC, Options.builder().backend("tpmarkdown").build());
            assertThat(markdown.contains("class=\"tabs"), is(false));
            assertThat(markdown.contains("CodeRay"), is(false));
        }
    }
}
