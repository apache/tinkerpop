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
import org.asciidoctor.SafeMode;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Probe for how {@link MarkdownConverter} handles core node types, including the crucial case of a
 * {@code pass} block whose raw content (produced by the treeprocessor for a gremlin tab group) must
 * survive verbatim into the Markdown output.
 */
public class MarkdownConverterProbeTest {

    private String toMarkdown(final String adoc) {
        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.javaConverterRegistry().register(MarkdownConverter.class);
            return asciidoctor.convert(adoc,
                    Options.builder().safe(SafeMode.UNSAFE).backend("tpmarkdown").build());
        }
    }

    @Test
    public void descriptionListRendersLinearlyWithoutBlowup() {
        // Regression guard: a description (definition) list previously fell through to a blanket
        // getContent() fallback that re-rendered subtrees, producing an output that grew far beyond
        // its input and exhausted the heap on dense books. Output must stay proportional to input.
        final StringBuilder src = new StringBuilder("= D\n\n");
        for (int i = 0; i < 200; i++) {
            src.append("term").append(i).append(":: definition text for entry ").append(i).append("\n");
        }
        final String md = toMarkdown(src.toString());
        // Linear: a dense dlist must not explode (input ~7KB; allow generous headroom but bar blowup).
        assertThat("dlist output must stay proportional to input, was " + md.length(),
                md.length() < src.length() * 5, is(true));
        assertThat(md, containsString("**term0**"));
        assertThat(md, containsString("definition text for entry 0"));
        assertThat(md, containsString("**term199**"));
    }

    @Test
    public void nestedListRendersAllItemsOnce() {
        final String md = toMarkdown("= D\n\n* a\n** a1\n** a2\n* b\n");
        assertThat(md, containsString("- a"));
        assertThat(md, containsString("- b"));
        assertThat(md, containsString("a1"));
        assertThat(md, containsString("a2"));
        // No item may be duplicated by re-conversion.
        assertThat(countOccurrences(md, "- b"), is(1));
    }

    private static int countOccurrences(final String haystack, final String needle) {
        int c = 0, i = 0;
        while ((i = haystack.indexOf(needle, i)) >= 0) { c++; i += needle.length(); }
        return c;
    }

    @Test
    public void rendersHeadingsAndParagraphs() {
        final String md = toMarkdown("= Title\n\n== Section One\n\nHello world.\n\n=== Sub\n\nMore text.\n");
        assertThat(md, containsString("# Title"));
        assertThat(md, containsString("## Section One"));
        assertThat(md, containsString("### Sub"));
        assertThat(md, containsString("Hello world."));
    }

    @Test
    public void llmsSummaryAttributeBecomesHiddenCommentNotBodyText() {
        // A curated [llms-summary="..."] on a section is emitted as a hidden HTML comment for the
        // llms.txt generator, and must NOT appear as visible body prose.
        final String md = toMarkdown("= T\n\n[llms-summary=\"Combine two lists element-wise.\"]\n"
                + "== Combine Step\n\nThe combine() step merges lists.\n");
        assertThat(md, containsString("<!-- llms-summary: Combine two lists element-wise. -->"));
        // The section's real prose still renders.
        assertThat(md, containsString("The combine() step merges lists."));
        // The summary text appears ONLY inside the comment, not as a standalone paragraph.
        assertThat(md.replace("<!-- llms-summary: Combine two lists element-wise. -->", ""),
                not(containsString("Combine two lists element-wise.")));
    }

    @Test
    public void noLlmsSummaryCommentWhenAttributeAbsent() {
        final String md = toMarkdown("= T\n\n== Plain Step\n\nSome prose.\n");
        assertThat(md, not(containsString("llms-summary")));
    }

    @Test
    public void emitsExplicitAnchorFromSectionId() {
        // An explicit AsciiDoc id must surface as an HTML anchor immediately before the heading so
        // xrefs of the form [label](#the-id) resolve.
        final String md = toMarkdown("= T\n\n[#my-anchor]\n== Custom Section\n\nBody.\n");
        assertThat(md, containsString("<a id=\"my-anchor\"></a>"));
        assertThat(md, containsString("## Custom Section"));
    }

    @Test
    public void emitsAutoGeneratedAnchorFromSectionId() {
        // Even without an explicit id, AsciiDoc auto-generates section ids (e.g. _custom_section);
        // these must be anchored too so auto-xrefs resolve after the page split.
        final String md = toMarkdown("= T\n\n== Custom Section\n\nBody.\n");
        assertThat(md, containsString("<a id=\"_custom_section\"></a>"));
    }

    @Test
    public void passBlockContentSurvivesVerbatim() {
        // Simulate what the treeprocessor emits for a gremlin tab group in markdown mode: a pass
        // block whose body is pre-rendered Markdown. It must appear unchanged in the output.
        final String adoc = "= T\n\n++++\n**console (groovy)**\n\n```text\ngremlin> g.V()\n```\n++++\n";
        final String md = toMarkdown(adoc);
        assertThat(md, containsString("**console (groovy)**"));
        assertThat(md, containsString("```text\ngremlin> g.V()\n```"));
    }

    private String toMarkdownWithImagesdir(final String adoc, final String imagesdir) {
        try (final Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.javaConverterRegistry().register(MarkdownConverter.class);
            return asciidoctor.convert(adoc, Options.builder().safe(SafeMode.UNSAFE).backend("tpmarkdown")
                    .attributes(org.asciidoctor.Attributes.builder().attribute("imagesdir", imagesdir).build())
                    .build());
        }
    }

    @Test
    public void blockImageTargetPrefixedWithImagesdir() {
        final String md = toMarkdownWithImagesdir("= T\n\nimage::match-step.png[match step]\n", "../images");
        assertThat(md, containsString("![match step](../images/match-step.png)"));
    }

    @Test
    public void inlineImageTargetPrefixedWithImagesdir() {
        final String md = toMarkdownWithImagesdir("= T\n\nSee image:logo.png[Logo] here.\n", "../../images");
        assertThat(md, containsString("![Logo](../../images/logo.png)"));
    }

    @Test
    public void absoluteAndUrlImageTargetsAreNotPrefixed() {
        final String abs = toMarkdownWithImagesdir("= T\n\nimage::/static/x.png[x]\n", "../images");
        assertThat(abs, containsString("![x](/static/x.png)"));
        final String url = toMarkdownWithImagesdir("= T\n\nimage::https://ex.com/y.png[y]\n", "../images");
        assertThat(url, containsString("![y](https://ex.com/y.png)"));
    }

    @Test
    public void rendersUnorderedList() {
        final String md = toMarkdown("= T\n\n* one\n* two\n* three\n");
        assertThat(md, containsString("- one"));
        assertThat(md, containsString("- two"));
    }

    @Test
    public void rendersInlineFormatting() {
        final String md = toMarkdown("= T\n\nSome *bold* and `code` and _italic_.\n");
        assertThat(md, containsString("`code`"));
        assertThat(md, containsString("**bold**"));
        assertThat(md, containsString("_italic_"));
    }
}
