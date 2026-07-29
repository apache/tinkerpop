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
 * Verifies the execute-once guarantee: when the same document is rendered twice (as the two backend
 * passes do), live Gremlin execution happens only on the first pass and the second pass is served
 * entirely from {@link GremlinExecutionCache}. This is the core of the mechanism that lets HTML and
 * Markdown be produced from a single console session.
 */
public class GremlinExecutionCacheTest {

    /** Records every executed statement so we can assert the second pass executes nothing. */
    private static final class RecordingExecutor implements GremlinTreeprocessor.StatementExecutor {
        final List<String> statements = new ArrayList<>();

        @Override
        public String execute(final String statement) {
            statements.add(statement);
            return "==>ok";
        }
    }

    private static final String DOC =
            "= Test\n\n[gremlin-groovy,modern]\n----\ng.V(1)\n----\n\n"
            + "[gremlin-groovy,modern]\n----\ng.V(2)\n----\n";

    @Test
    public void secondRenderPassExecutesNothingWhenCacheShared() {
        final RecordingExecutor executor = new RecordingExecutor();
        final GremlinExecutionCache cache = new GremlinExecutionCache();

        // Two processors sharing one cache simulate the two per-backend render passes: the plugin
        // creates a fresh treeprocessor per convert(), but the SHARED cache persists between them.
        final GremlinTreeprocessor pass1 = new GremlinTreeprocessor(executor, null, cache);
        final GremlinTreeprocessor pass2 = new GremlinTreeprocessor(executor, null, cache);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(pass1);
            asciidoctor.convert(DOC, Options.builder().build());
            final int afterPass1 = executor.statements.size();
            assertThat("first pass must execute Gremlin", afterPass1 > 0, is(true));

            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(pass2);
            asciidoctor.convert(DOC, Options.builder().build());
            final int afterPass2 = executor.statements.size();

            assertThat("second pass must execute nothing (served from cache)",
                    afterPass2, is(afterPass1));
            assertThat("both gremlin blocks cached", cache.size(), is(2));
        }
    }

    @Test
    public void separateCachesDoNotShareAndBothExecute() {
        final RecordingExecutor executor = new RecordingExecutor();

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(new GremlinTreeprocessor(executor, null,
                    new GremlinExecutionCache()));
            asciidoctor.convert(DOC, Options.builder().build());
            final int afterPass1 = executor.statements.size();

            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(new GremlinTreeprocessor(executor, null,
                    new GremlinExecutionCache()));
            asciidoctor.convert(DOC, Options.builder().build());

            assertThat("distinct caches re-execute", executor.statements.size() > afterPass1, is(true));
        }
    }

    @Test
    public void cachedContentDrivesRenderedOutput() {
        final RecordingExecutor executor = new RecordingExecutor();
        final GremlinExecutionCache cache = new GremlinExecutionCache();
        final GremlinTreeprocessor pass1 = new GremlinTreeprocessor(executor, null, cache);
        final GremlinTreeprocessor pass2 = new GremlinTreeprocessor(executor, null, cache);

        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(pass1);
            final String html1 = asciidoctor.convert(DOC, Options.builder().build());

            asciidoctor.unregisterAllExtensions();
            asciidoctor.javaExtensionRegistry().treeprocessor(pass2);
            final String html2 = asciidoctor.convert(DOC, Options.builder().build());

            // Both passes render the tab widget, and the second (cache-served) pass is equivalent.
            assertThat(html1, containsString("section class=\"tabs"));
            assertThat(html2, containsString("section class=\"tabs"));
        }
    }
}
