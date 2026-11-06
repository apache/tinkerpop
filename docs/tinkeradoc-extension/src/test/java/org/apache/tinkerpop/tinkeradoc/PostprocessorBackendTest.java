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
import org.asciidoctor.Attributes;
import org.asciidoctor.Options;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Verifies the postprocessor behaves correctly per backend: the {@code x.y.z} version placeholder
 * is substituted in BOTH HTML and Markdown output, while the CodeRay empty-comment-span cleanup is
 * applied only to HTML.
 */
public class PostprocessorBackendTest {

    private String convert(final String backend) {
        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.javaConverterRegistry().register(MarkdownConverter.class);
            asciidoctor.javaExtensionRegistry().postprocessor(GremlinPostprocessor.class);
            final String input = "= Test\n\nDownload version x.y.z from the repo.\n";
            final Options options = Options.builder()
                    .backend(backend)
                    .attributes(Attributes.builder().attribute("tinkerpop-version", "3.7.7").build())
                    .build();
            return asciidoctor.convert(input, options);
        }
    }

    @Test
    public void versionSubstitutedInHtml() {
        final String html = convert("html5");
        assertThat(html, containsString("3.7.7"));
        assertThat(html, not(containsString("x.y.z")));
    }

    @Test
    public void versionSubstitutedInMarkdown() {
        final String md = convert("tpmarkdown");
        assertThat(md, containsString("3.7.7"));
        assertThat(md, not(containsString("x.y.z")));
    }

    @Test
    public void codeRayCleanupSkippedForMarkdown() {
        // A literal CodeRay empty-comment-span in Markdown content must be left untouched (the
        // cleanup is an HTML artifact); in practice Markdown never contains these, but this pins
        // the backend gate so the cleanup cannot silently corrupt Markdown.
        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.javaConverterRegistry().register(MarkdownConverter.class);
            asciidoctor.javaExtensionRegistry().postprocessor(GremlinPostprocessor.class);
            // A passthrough block carries the span verbatim into the output.
            final String input = "= T\n\n++++\nkeep<span class=\"comment\">/* */</span>me\n++++\n";
            final String md = asciidoctor.convert(input, Options.builder().backend("tpmarkdown").build());
            assertThat(md, containsString("<span class=\"comment\">/* */</span>"));
        }
    }
}
