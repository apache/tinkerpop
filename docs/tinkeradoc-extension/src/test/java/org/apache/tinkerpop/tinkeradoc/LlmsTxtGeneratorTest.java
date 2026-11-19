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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link LlmsTxtGenerator}: the llms.txt discovery-index format and its title/description
 * extraction.
 */
public class LlmsTxtGeneratorTest {

    private final LlmsTxtGenerator gen =
            new LlmsTxtGenerator("Apache TinkerPop Documentation", "Docs for a graph computing framework.");

    @Test
    public void emitsH1AndSummaryBlockquote() {
        final String out = gen.generate(Arrays.asList(
                new LlmsTxtGenerator.Entry("dev/io/index.md", "IO Reference", "How IO works.")));
        assertThat(out, containsString("# Apache TinkerPop Documentation\n"));
        assertThat(out, containsString("> Docs for a graph computing framework.\n"));
    }

    @Test
    public void groupsPagesByBookAsSectionsWithLinks() {
        final List<LlmsTxtGenerator.Entry> entries = Arrays.asList(
                new LlmsTxtGenerator.Entry("dev/io/index.md", "IO Reference", "IO overview."),
                new LlmsTxtGenerator.Entry("dev/io/graphson.md", "GraphSON", "A JSON format."),
                new LlmsTxtGenerator.Entry("reference/index.md", "Reference", "The reference."));
        final String out = gen.generate(entries);
        assertThat(out, containsString("## dev/io\n"));
        assertThat(out, containsString("## reference\n"));
        assertThat(out, containsString("- [GraphSON](dev/io/graphson.md): A JSON format."));
        assertThat(out, containsString("- [IO Reference](dev/io/index.md): IO overview."));
    }

    @Test
    public void landingPageListedFirstInItsSection() {
        final List<LlmsTxtGenerator.Entry> entries = Arrays.asList(
                new LlmsTxtGenerator.Entry("dev/io/graphson.md", "GraphSON", "g"),
                new LlmsTxtGenerator.Entry("dev/io/index.md", "IO Reference", "i"));
        final String out = gen.generate(entries);
        final int idx = out.indexOf("dev/io/index.md");
        final int gs = out.indexOf("dev/io/graphson.md");
        assertThat("landing page must come first", idx < gs, is(true));
    }

    private static List<String> paths(final List<LlmsTxtGenerator.Entry> entries) {
        return entries.stream().map(e -> e.relativePath).collect(Collectors.toList());
    }

    @Test
    public void pagesAreListedInDocumentOrderNotFileNameOrder() {
        // File-name order would be providers, release, line: the reverse of the book.
        final List<LlmsTxtGenerator.Entry> entries = Arrays.asList(
                new LlmsTxtGenerator.Entry("upgrade/v3.5-v3.5-providers.md", "Upgrading for Providers", "p"),
                new LlmsTxtGenerator.Entry("upgrade/v3.5-v3.5.md", "TinkerPop 3.5.0", "r",
                        List.of("v3.5-v3.5-providers.md")),
                new LlmsTxtGenerator.Entry("upgrade/v3.5.md", "TinkerPop 3.5.0", "l", List.of("v3.5-v3.5.md")),
                new LlmsTxtGenerator.Entry("upgrade/v3.7.md", "TinkerPop 3.7.0", "l"),
                new LlmsTxtGenerator.Entry("upgrade/index.md", "Upgrade", "i", List.of("v3.7.md", "v3.5.md")));
        assertThat(paths(LlmsTxtGenerator.inDocumentOrder(entries)), is(List.of("upgrade/index.md",
                "upgrade/v3.7.md", "upgrade/v3.5.md", "upgrade/v3.5-v3.5.md", "upgrade/v3.5-v3.5-providers.md")));
    }

    @Test
    public void pagesNoChildLinkReachesAreAppendedInPathOrder() {
        final List<LlmsTxtGenerator.Entry> entries = Arrays.asList(
                new LlmsTxtGenerator.Entry("b/z.md", "Z", "z"),
                new LlmsTxtGenerator.Entry("b/y.md", "Y", "y"),
                new LlmsTxtGenerator.Entry("b/x.md", "X", "x"),
                new LlmsTxtGenerator.Entry("b/index.md", "B", "b", List.of("z.md", "missing.md")));
        assertThat(paths(LlmsTxtGenerator.inDocumentOrder(entries)),
                is(List.of("b/index.md", "b/z.md", "b/x.md", "b/y.md")));
    }

    @Test
    public void extractsOnlyTheSplittersChildLinks() {
        final String content = "# Parent\n\n[Read more](child-a.md)\n\nSee [Read more](inline.md) here.\n\n"
                + "[Other](other.md)\n\n[Read more](../other-book/x.md)\n\n```\n[Read more](fenced.md)\n```\n\n"
                + "[Read more](child-b.md)\n";
        assertThat(LlmsTxtGenerator.extractChildPages(content), is(List.of("child-a.md", "child-b.md")));
    }

    @Test
    public void splitBookIsIndexedInTheOrderOfItsSections() {
        // End to end over real splitter output.
        final String md = "preamble\n\n"
                + "<a id=\"_upgrade\"></a>\n# TinkerPop Upgrade Information\n<!-- llms-summary: Upgrades. -->\n\nintro\n\n"
                + "<a id=\"_tinkerpop_3_5_0\"></a>\n# TinkerPop 3.5.0\n<!-- llms-summary: The 3.5.x line. -->\n\n"
                + "<a id=\"_tinkerpop_3_5_0_2\"></a>\n## TinkerPop 3.5.0\n<!-- llms-summary: The 3.5.0 release. -->\n\n"
                + "<a id=\"_upgrading_for_providers\"></a>\n### Upgrading for Providers\n"
                + "<!-- llms-summary: 3.5.0 providers. -->\n\nnotes\n";
        final List<LlmsTxtGenerator.Entry> entries = new ArrayList<>();
        for (final MarkdownSplitter.Page p : new MarkdownSplitter().split(md, "index.md")) {
            final String rel = "upgrade/" + p.getFileName();
            entries.add(new LlmsTxtGenerator.Entry(rel, LlmsTxtGenerator.extractTitle(p.getContent(), rel),
                    LlmsTxtGenerator.extractDescription(p.getContent()),
                    LlmsTxtGenerator.extractChildPages(p.getContent())));
        }
        final String out = gen.generate(entries);
        final int line = out.indexOf("The 3.5.x line.");
        final int release = out.indexOf("The 3.5.0 release.");
        final int providers = out.indexOf("3.5.0 providers.");
        assertThat(line > 0 && line < release && release < providers, is(true));
    }

    @Test
    public void linkPrefixIsPrependedToEveryPagePath() {
        final LlmsTxtGenerator prefixed = new LlmsTxtGenerator(
                "Apache TinkerPop Documentation", "Docs.", "docs/3.7.7/");
        final String out = prefixed.generate(Arrays.asList(
                new LlmsTxtGenerator.Entry("dev/io/graphson.md", "GraphSON", "A JSON format.")));
        assertThat(out, containsString("- [GraphSON](docs/3.7.7/dev/io/graphson.md): A JSON format."));
    }

    @Test
    public void extractsTitleFromFirstHeading() {
        final String content = "> pointer\n\n<a id=\"graphson\"></a>\n# GraphSON\n\nGraphSON is a JSON format.\n";
        assertThat(LlmsTxtGenerator.extractTitle(content, "dev/io/graphson.md"), is("GraphSON"));
    }

    @Test
    public void titleFallsBackToFileNameWhenNoHeading() {
        assertThat(LlmsTxtGenerator.extractTitle("just prose, no heading\n", "dev/io/orphan.md"),
                is("orphan"));
    }

    @Test
    public void prefersCuratedLlmsSummaryOverProse() {
        final String content = "> For the complete documentation index, see [llms.txt](/llms.txt)\n\n"
                + "<a id=\"combine-step\"></a>\n### Combine Step\n\n"
                + "<!-- llms-summary: Combine two lists element-wise; handy for zipping results. -->\n\n"
                + "The combine()-step merges the incoming list with the provided list argument.\n";
        assertThat(LlmsTxtGenerator.extractDescription(content),
                is("Combine two lists element-wise; handy for zipping results."));
    }

    @Test
    public void curatedSummaryUsedVerbatimNotTruncatedToOneSentence() {
        final String content = "<a id=\"x\"></a>\n# X\n\n"
                + "<!-- llms-summary: First clause. Second clause stays too. -->\n\nProse.\n";
        // Unlike the prose heuristic (one sentence), curated text is used as authored.
        assertThat(LlmsTxtGenerator.extractDescription(content),
                is("First clause. Second clause stays too."));
    }

    @Test
    public void fallsBackToProseWhenNoCuratedSummary() {
        final String content = "<a id=\"x\"></a>\n# X\n\nPlain prose description here. More detail.\n";
        assertThat(LlmsTxtGenerator.extractDescription(content), is("Plain prose description here."));
    }

    @Test
    public void extractsDescriptionFromFirstProseParagraph() {
        final String content = "> For the complete documentation index, see [llms.txt](/llms.txt)\n\n"
                + "<a id=\"graphson\"></a>\n# GraphSON\n\n"
                + "GraphSON is a `JSON`-based [format](x.html) designed for humans. More detail follows.\n";
        final String desc = LlmsTxtGenerator.extractDescription(content);
        assertThat(desc, is("GraphSON is a JSON-based format designed for humans."));
    }

    @Test
    public void descriptionSkipsStructuralLinesLikeFencesAndImages() {
        final String content = "<a id=\"x\"></a>\n# X\n\n![img](a.png)\n\n```java\ncode\n```\n\nActual prose here.\n";
        assertThat(LlmsTxtGenerator.extractDescription(content), is("Actual prose here."));
    }

    @Test
    public void emptyDescriptionWhenNoProse() {
        final String content = "<a id=\"x\"></a>\n# X\n\n```\ncode\n```\n";
        assertThat(LlmsTxtGenerator.extractDescription(content), is(""));
    }
}
