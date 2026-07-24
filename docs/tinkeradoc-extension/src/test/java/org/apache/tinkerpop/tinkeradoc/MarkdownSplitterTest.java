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
 * Tests for {@link MarkdownSplitter}: the size-driven, heading-aligned page splitter and its
 * cross-page link rewriting.
 */
public class MarkdownSplitterTest {

    private static String heading(final String id, final int level, final String title) {
        final StringBuilder h = new StringBuilder();
        for (int i = 0; i < level; i++) h.append('#');
        return "<a id=\"" + id + "\"></a>\n" + h + " " + title + "\n";
    }

    private static String filler(final int bytes) {
        final StringBuilder sb = new StringBuilder();
        while (sb.length() < bytes) sb.append("lorem ipsum dolor sit amet consectetur\n");
        return sb.toString();
    }

    private static List<String> fileNames(final List<MarkdownSplitter.Page> pages) {
        return pages.stream().map(MarkdownSplitter.Page::getFileName).collect(Collectors.toList());
    }

    @Test
    public void smallBookStaysSinglePage() {
        final String md = heading("_io_reference", 1, "IO Reference") + "\nsome intro\n\n"
                + heading("graphml", 1, "GraphML") + "\nshort content\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter(50_000).split(md, "index.md");
        assertThat(pages.size(), is(1));
        assertThat(pages.get(0).getFileName(), is("index.md"));
        assertThat(pages.get(0).getContent(), containsString("# GraphML"));
    }

    @Test
    public void oversizedSectionBecomesOwnPage() {
        final String md = heading("_io_reference", 1, "IO Reference") + "\nintro\n\n"
                + heading("graphml", 1, "GraphML") + "\nshort\n\n"
                + heading("graphson", 1, "GraphSON") + "\n" + filler(60_000);
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter(50_000).split(md, "index.md");
        final List<String> names = fileNames(pages);
        // The big GraphSON chapter must be split off into graphson.md; index keeps the small parts.
        assertThat(names, hasItem("index.md"));
        assertThat(names, hasItem("graphson.md"));
        final MarkdownSplitter.Page index = pages.stream()
                .filter(p -> p.getFileName().equals("index.md")).findFirst().orElseThrow(AssertionError::new);
        assertThat(index.getContent(), containsString("# GraphML"));
        assertThat(index.getContent(), not(containsString(filler(60_000).substring(0, 200))));
    }

    @Test
    public void everyDivisiblePageUnderBudget() {
        // A, B, C each have small children? No — here each is a single leaf heading. B alone is
        // 60KB of direct content, which is indivisible: it must occupy one page even though that
        // page exceeds the budget (there is no finer heading to split on). Every OTHER page must
        // be under budget.
        final String md = heading("book", 1, "Book") + "\nintro\n\n"
                + heading("a", 1, "A") + "\n" + filler(40_000)
                + heading("b", 1, "B") + "\n" + filler(60_000)
                + heading("c", 1, "C") + "\n" + filler(20_000);
        final int budget = 50_000;
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter(budget).split(md, "index.md");
        for (final MarkdownSplitter.Page p : pages) {
            final boolean indivisible = p.getFileName().equals("b.md");
            if (!indivisible) {
                assertThat(p.getFileName() + " over budget: " + p.getContent().length(),
                        p.getContent().length() <= budget, is(true));
            }
        }
        // The indivisible 60KB leaf is isolated on its own page (so it doesn't bloat neighbors).
        assertThat(fileNames(pages), hasItem("b.md"));
    }

    @Test
    public void renderedPageIncludingPointerStaysUnderBudget() {
        // Every divisible page's FINAL content (which includes the prepended llms.txt pointer) must
        // stay within the budget: the splitter reserves room for the pointer when packing.
        final int budget = 50_000;
        final StringBuilder md = new StringBuilder(heading("book", 1, "Book") + "\nintro\n\n");
        // Many medium sections that would pack right up against the budget.
        for (int i = 0; i < 8; i++) {
            md.append(heading("sec-" + i, 1, "Section " + i)).append('\n').append(filler(12_000));
        }
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter(budget).split(md.toString(), "index.md");
        for (final MarkdownSplitter.Page p : pages) {
            assertThat(p.getFileName() + " (incl pointer) = " + p.getContent().length(),
                    p.getContent().length() <= budget, is(true));
            assertThat(p.getContent(), containsString("[llms.txt](/llms.txt)"));
        }
    }

    @Test
    public void indivisibleOversizedLeafIsIsolatedOnOwnPage() {
        final String md = heading("book", 1, "Book") + "\nintro\n\n"
                + heading("small", 1, "Small") + "\nshort\n\n"
                + heading("huge", 1, "Huge") + "\n" + filler(70_000);
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter(50_000).split(md, "index.md");
        final MarkdownSplitter.Page huge = pages.stream()
                .filter(p -> p.getFileName().equals("huge.md")).findFirst().orElseThrow(AssertionError::new);
        // The huge leaf page contains only the Huge section, not the small one.
        assertThat(huge.getContent(), containsString("# Huge"));
        assertThat(huge.getContent(), not(containsString("# Small")));
    }

    @Test
    public void descendsToChildrenWhenChapterTooBig() {
        // A chapter (90KB total) that exceeds the budget descends to its h2 children. Greedy packing
        // keeps pages full: the chapter page leads with the chapter heading + as many whole child
        // sections as fit; the remainder overflow to further pages. So we get chapter.md (heading +
        // Section One) and a second page for Section Two — never a mid-section cut.
        final String md = heading("book", 1, "Book") + "\nintro\n\n"
                + heading("chapter", 1, "Chapter") + "\nlead-in\n\n"
                + heading("sec-one", 2, "Section One") + "\n" + filler(45_000)
                + heading("sec-two", 2, "Section Two") + "\n" + filler(45_000);
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter(50_000).split(md, "index.md");
        final List<String> names = fileNames(pages);
        assertThat(names, hasItem("chapter.md"));
        // Section Two overflows to its own page (named from its anchor); each h2 stays whole.
        assertThat(names, hasItem("sec-two.md"));
        final MarkdownSplitter.Page chapter = pages.stream()
                .filter(p -> p.getFileName().equals("chapter.md")).findFirst().orElseThrow(AssertionError::new);
        assertThat(chapter.getContent(), containsString("# Chapter"));
        assertThat(chapter.getContent(), containsString("## Section One"));
        assertThat(chapter.getContent(), not(containsString("## Section Two")));
    }

    @Test
    public void rewritesCrossPageLinksAndKeepsSamePageBare() {
        final String md = heading("book", 1, "Book") + "\nintro\n\n"
                + heading("graphml", 1, "GraphML") + "\nSee [GraphSON](#graphson) and [self](#graphml).\n\n"
                + heading("graphson", 1, "GraphSON") + "\n" + filler(60_000)
                + "Back to [GraphML](#graphml).\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter(50_000).split(md, "index.md");

        final MarkdownSplitter.Page index = pages.stream()
                .filter(p -> p.getFileName().equals("index.md")).findFirst().orElseThrow(AssertionError::new);
        final MarkdownSplitter.Page graphson = pages.stream()
                .filter(p -> p.getFileName().equals("graphson.md")).findFirst().orElseThrow(AssertionError::new);

        // GraphML lives on index; its link to GraphSON (another page) is rewritten; self-link stays bare.
        assertThat(index.getContent(), containsString("[GraphSON](graphson.md#graphson)"));
        assertThat(index.getContent(), containsString("[self](#graphml)"));
        // GraphSON page links back to GraphML on the index page.
        assertThat(graphson.getContent(), containsString("[GraphML](index.md#graphml)"));
    }

    @Test
    public void hashCommentsInsideCodeFenceAreNotTreatedAsHeadings() {
        // A code fence containing shell/properties comment lines that begin with '#' must NOT be
        // parsed as section headings (which would split the page mid-code-block and produce generic
        // "section.md" file names). The whole fenced block stays with its owning section.
        final String md = heading("book", 1, "Book") + "\nintro\n\n"
                + heading("config", 1, "Config") + "\nHere is a config file:\n\n"
                + "```properties\n# Spark Configuration\nspark.master=local[4]\n# another comment\n"
                + "spark.executor.memory=1g\n```\n\nAfter the block.\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter(50_000).split(md, "index.md");
        final List<String> names = fileNames(pages);
        // Only index.md — no page split off a code comment, no generic "section*.md".
        assertThat(names.stream().noneMatch(n -> n.startsWith("section")), is(true));
        final String body = pages.get(0).getContent();
        // The fenced block and its comments remain intact and are not promoted to headings.
        assertThat(body, containsString("# Spark Configuration"));
        assertThat(body, containsString("```properties"));
        assertThat(body, containsString("After the block."));
    }

    @Test
    public void unknownAnchorsLeftUnchanged() {
        final String md = heading("book", 1, "Book") + "\nSee [external](#not-a-heading).\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter(50_000).split(md, "index.md");
        assertThat(pages.get(0).getContent(), containsString("[external](#not-a-heading)"));
    }
}
