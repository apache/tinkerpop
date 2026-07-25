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

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link MarkdownSplitter}, which splits a rendered book into pages driven solely by
 * {@code llms-summary} markers: a section becomes its own page iff it carries a summary. Unsummarized
 * sections attach to their nearest summarized ancestor's page. There is no size-based splitting; the
 * byte budget is only a lint, with intentional exceptions flagged {@code allow-oversize}.
 */
public class MarkdownSplitterTest {

    /** A heading with an anchor, no summary (does NOT start a page). */
    private static String heading(final String id, final int level, final String title) {
        final StringBuilder h = new StringBuilder();
        for (int i = 0; i < level; i++) h.append('#');
        return "<a id=\"" + id + "\"></a>\n" + h + " " + title + "\n";
    }

    /** A heading with an anchor and an llms-summary marker (starts its own page). */
    private static String summarized(final String id, final int level, final String title, final String summary) {
        return heading(id, level, title) + "<!-- llms-summary: " + summary + " -->\n";
    }

    private static String filler(final int bytes) {
        final StringBuilder sb = new StringBuilder();
        while (sb.length() < bytes) sb.append("lorem ipsum dolor sit amet consectetur\n");
        return sb.toString();
    }

    private static List<String> fileNames(final List<MarkdownSplitter.Page> pages) {
        return pages.stream().map(MarkdownSplitter.Page::getFileName).collect(Collectors.toList());
    }

    private static MarkdownSplitter.Page page(final List<MarkdownSplitter.Page> pages, final String name) {
        return pages.stream().filter(p -> p.getFileName().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError("no page named " + name + " in " + fileNames(pages)));
    }

    // A "book" in rendered Markdown is a flat sequence of level-1 (#) sections: the doctitle first,
    // then chapters as siblings (AsciiDoc renders a book's doctitle and its level-0 chapters all as
    // <h1>). The document root (index.md) owns the preamble and the doctitle section's content up to
    // the first summarized sibling.

    @Test
    public void bookWithNoSummariesIsASinglePage() {
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference") + "\nintro\n\n"
                + heading("a", 1, "A") + "\ncontent a\n\n"
                + heading("b", 1, "B") + "\ncontent b\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter().split(md, "index.md");
        assertThat(pages.size(), is(1));
        assertThat(pages.get(0).getFileName(), is("index.md"));
        assertThat(pages.get(0).getContent(), containsString("# A"));
        assertThat(pages.get(0).getContent(), containsString("content b"));
    }

    @Test
    public void eachSummarizedSectionBecomesItsOwnPage() {
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference") + "\nintro\n\n"
                + summarized("a", 1, "A", "Section A.") + "\ncontent a\n\n"
                + summarized("b", 1, "B", "Section B.") + "\ncontent b\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter().split(md, "index.md");
        assertThat(fileNames(pages), hasItem("index.md"));
        assertThat(fileNames(pages), hasItem("a.md"));
        assertThat(fileNames(pages), hasItem("b.md"));
        assertThat(page(pages, "a.md").getContent(), containsString("content a"));
        assertThat(page(pages, "a.md").getContent(), not(containsString("content b")));
        // The index holds the preamble + the (unsummarized) doctitle section, not the summarized ones.
        assertThat(page(pages, "index.md").getContent(), containsString("intro"));
        assertThat(page(pages, "index.md").getContent(), not(containsString("content a")));
    }

    @Test
    public void unsummarizedChildAttachesToSummarizedAncestorPage() {
        // Chapter is summarized (own page); its child A1 is not (stays on chapter page); its child A2
        // is summarized (breaks off).
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference") + "\nintro\n\n"
                + summarized("chapter", 1, "Chapter", "A chapter.") + "\nlead\n\n"
                + heading("a1", 2, "A1") + "\ncontent a1\n\n"
                + summarized("a2", 2, "A2", "Subsection A2.") + "\ncontent a2\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter().split(md, "index.md");
        assertThat(fileNames(pages), hasItem("chapter.md"));
        assertThat(fileNames(pages), hasItem("a2.md"));
        assertThat(fileNames(pages), not(hasItem("a1.md")));
        assertThat(page(pages, "chapter.md").getContent(), containsString("## A1"));
        assertThat(page(pages, "chapter.md").getContent(), containsString("content a1"));
        assertThat(page(pages, "chapter.md").getContent(), not(containsString("content a2")));
        assertThat(page(pages, "a2.md").getContent(), containsString("content a2"));
    }

    @Test
    public void catalogWithPerChildSummariesYieldsOnePagePerChild() {
        // A "catalog" is simply a summarized parent whose children are each summarized: no special
        // marker needed — summaries alone drive the per-child split.
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference") + "\nintro\n\n"
                + summarized("steps", 1, "Steps", "The catalog.") + "\ncatalog intro\n\n"
                + summarized("fold-step", 2, "Fold Step", "fold() aggregates.") + "\nfold body\n\n"
                + summarized("group-step", 2, "Group Step", "group() organizes.") + "\ngroup body\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter().split(md, "index.md");
        assertThat(fileNames(pages), hasItem("steps.md"));
        assertThat(fileNames(pages), hasItem("fold-step.md"));
        assertThat(fileNames(pages), hasItem("group-step.md"));
        assertThat(page(pages, "fold-step.md").getContent(), containsString("fold body"));
        assertThat(page(pages, "fold-step.md").getContent(), not(containsString("group body")));
        assertThat(page(pages, "steps.md").getContent(), containsString("catalog intro"));
    }

    @Test
    public void keepWholeIsJustAbsenceOfChildSummaries() {
        // A summarized section whose (large) children are NOT summarized stays one whole page.
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference") + "\nintro\n\n"
                + summarized("version-2", 1, "Version 2.0", "GraphSON 2.0.") + "\nversion intro\n\n"
                + heading("edge", 2, "Edge") + "\n" + filler(30_000)
                + heading("vertex", 2, "Vertex") + "\n" + filler(30_000);
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter().split(md, "index.md");
        assertThat(fileNames(pages), hasItem("version-2.md"));
        assertThat(fileNames(pages), not(hasItem("edge.md")));
        final MarkdownSplitter.Page v2 = page(pages, "version-2.md");
        assertThat(v2.getContent(), containsString("## Edge"));
        assertThat(v2.getContent(), containsString("## Vertex"));
    }

    @Test
    public void rewritesCrossPageLinksAndKeepsSamePageBare() {
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference") + "\nintro\n\n"
                + summarized("graphml", 1, "GraphML", "GraphML.")
                + "\nSee [GraphSON](#graphson) and [self](#graphml).\n\n"
                + summarized("graphson", 1, "GraphSON", "GraphSON.") + "\nBack to [GraphML](#graphml).\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter().split(md, "index.md");
        assertThat(page(pages, "graphml.md").getContent(), containsString("[GraphSON](graphson.md#graphson)"));
        assertThat(page(pages, "graphml.md").getContent(), containsString("[self](#graphml)"));
        assertThat(page(pages, "graphson.md").getContent(), containsString("[GraphML](graphml.md#graphml)"));
    }

    @Test
    public void unknownAnchorsLeftUnchanged() {
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference")
                + "\nSee [external](#not-a-heading).\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter().split(md, "index.md");
        assertThat(pages.get(0).getContent(), containsString("[external](#not-a-heading)"));
    }

    @Test
    public void hashCommentsInsideCodeFenceAreNotTreatedAsHeadings() {
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference") + "\nintro\n\n"
                + summarized("config", 1, "Config", "Config file.") + "\nHere is a config file:\n\n"
                + "```properties\n# Spark Configuration\nspark.master=local[4]\n# another comment\n"
                + "spark.executor.memory=1g\n```\n\nAfter the block.\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter().split(md, "index.md");
        assertThat(fileNames(pages).stream().noneMatch(n -> n.startsWith("section")), is(true));
        final String body = page(pages, "config.md").getContent();
        assertThat(body, containsString("# Spark Configuration"));
        assertThat(body, containsString("```properties"));
        assertThat(body, containsString("After the block."));
    }
}
