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
import org.asciidoctor.SafeMode;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Integration test that processes the full fixture document through Asciidoctor with the
 * GremlinTreeprocessor (dry-run) and GremlinPostprocessor, verifying the output HTML.
 */
public class IntegrationTest {

    private static String html;

    @BeforeClass
    public static void processFixture() throws IOException {
        final String fixture;
        try (final InputStream is = IntegrationTest.class.getResourceAsStream("/integration-fixture.asciidoc")) {
            fixture = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        final GremlinTreeprocessor treeprocessor = new GremlinTreeprocessor();
        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.javaExtensionRegistry().treeprocessor(treeprocessor);
            final Options options = Options.builder()
                    .safe(SafeMode.UNSAFE)
                    .attributes(Attributes.builder()
                            .attribute("gremlin-docs-dryrun", "")
                            .attribute("tinkerpop-version", "3.7.7-SNAPSHOT")
                            .build())
                    .build();
            html = asciidoctor.convert(fixture, options);
        }
    }

    @Test
    public void shouldContainTabbedSectionsForGremlinBlocks() {
        assertThat(html, containsString("section class=\"tabs"));
    }

    @Test
    public void shouldContainGremlinPrompts() {
        assertThat(html, containsString("gremlin"));
        assertThat(html, containsString("g.V"));
    }

    @Test
    public void shouldContainConsoleTabLabel() {
        assertThat(html, containsString("console (groovy)"));
    }

    @Test
    public void shouldContainManualTabLabels() {
        assertThat(html, containsString(">java<"));
        assertThat(html, containsString(">python<"));
    }

    @Test
    public void shouldProduceMultiTabGroupForManualTabs() {
        // The gremlin block + java + python = 3 tabs
        assertThat(html, containsString("tabs tabs-3"));
    }

    @Test
    public void shouldProduceStandaloneTabGroup() {
        // groovy + java + csharp standalone tabs = 3 tabs
        assertThat(html, containsString(">groovy<"));
        assertThat(html, containsString(">csharp<"));
    }

    @Test
    public void shouldHandleBareGremlinBlock() {
        assertThat(html, containsString("integer"));
    }

    @Test
    public void shouldHandleExistingGraphBlock() {
        assertThat(html, containsString("count"));
    }

    @Test
    public void shouldHandleErrorBlock() {
        assertThat(html, containsString("invalid_syntax_here"));
    }

    @Test
    public void shouldHandleCallouts() {
        // Callouts in the [source,java] block should be processed by Asciidoctor
        assertThat(html, containsString("conum"));
    }

    @Test
    public void shouldReplaceVersionPlaceholder() {
        assertThat(html, containsString("3.7.7-SNAPSHOT"));
        assertThat(html, not(containsString("x.y.z")));
    }

    @Test
    public void shouldContainCodeRayHighlightStructure() {
        assertThat(html, containsString("CodeRay highlight"));
    }
}
