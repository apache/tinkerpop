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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link MarkdownSplitter}, which splits a rendered book into pages driven solely by
 * {@code llms-summary} markers: a section becomes its own page iff it carries a summary. Unsummarized
 * sections attach to their nearest summarized ancestor's page. There is no size-based splitting; the
 * byte budget is only a lint, with intentional exceptions flagged {@code allow-oversize}.
 */
public class MarkdownSplitterTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

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
    public void licenseCommentIsCarriedOntoEveryPageAfterThePointer() {
        final String md = MarkdownConverter.LICENSE_COMMENT + "preamble\n\n" + heading("io", 1, "IO Reference")
                + "\nintro\n\n" + summarized("a", 1, "A", "About A.") + "\ncontent a\n\n"
                + summarized("b", 1, "B", "About B.") + "\ncontent b\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter(50_000, "4.0.0").split(md, "index.md");
        assertThat(pages.size(), is(3));
        final String pointer = MarkdownSplitter.llmsPointer("4.0.0");
        for (final MarkdownSplitter.Page p : pages) {
            assertThat(p.getFileName(), p.getContent().startsWith(pointer + MarkdownConverter.LICENSE_COMMENT), is(true));
            assertThat(p.getContent().indexOf("Licensed to the Apache"),
                    is(p.getContent().lastIndexOf("Licensed to the Apache")));
        }
    }

    @Test
    public void bookWithoutLicenseCommentGetsNone() {
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference") + "\nintro\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter(50_000, "4.0.0").split(md, "index.md");
        assertThat(pages.get(0).getContent(), not(containsString("Licensed to the Apache")));
    }

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
    public void parentPageReferencesEachSpunOffChild() {
        // 2b: when a summarized child breaks off, the parent renders a table-of-contents entry in its
        // place -- the child's heading (kept at its level), its summary as prose, and a link to the
        // page -- so the parent stays a coherent outline instead of jumping over the missing content.
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference") + "\nintro\n\n"
                + summarized("graphml", 2, "GraphML", "The GraphML format.") + "\ncontent graphml\n\n"
                + heading("wrapup", 1, "Wrap Up") + "\nclosing words\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter().split(md, "index.md");
        final String index = page(pages, "index.md").getContent();
        // Heading kept at the child's own level, summary rendered as prose, and a link to its page.
        assertThat(index, containsString("## GraphML"));
        assertThat(index, containsString("The GraphML format."));
        assertThat(index, containsString("[Read more](graphml.md)"));
        // ...but not the child's body, which lives on its own page.
        assertThat(index, not(containsString("content graphml")));
        // The reference sits in the child's original position: after the intro, before the wrap-up.
        assertThat(index.indexOf("intro") < index.indexOf("## GraphML"), is(true));
        assertThat(index.indexOf("## GraphML") < index.indexOf("Wrap Up"), is(true));
    }

    @Test
    public void referenceCascadesThroughEveryLevel() {
        // The table-of-contents reference applies at every depth: a chapter that is itself its own
        // page must reference its own summarized subsections, so the whole book stays followable top
        // to bottom (index -> chapter -> subsection), not just from the landing page.
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference") + "\nintro\n\n"
                + summarized("traversal", 1, "The Traversal", "The step catalog.") + "\ncatalog intro\n\n"
                + summarized("fold-step", 2, "Fold Step", "fold() aggregates.") + "\nfold body\n\n"
                + summarized("group-step", 2, "Group Step", "group() organizes.") + "\ngroup body\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter().split(md, "index.md");
        // The landing page references the chapter...
        assertThat(page(pages, "index.md").getContent(), containsString("[Read more](traversal.md)"));
        // ...and the chapter page in turn references each of its own spun-off steps.
        final String traversal = page(pages, "traversal.md").getContent();
        assertThat(traversal, containsString("## Fold Step"));
        assertThat(traversal, containsString("[Read more](fold-step.md)"));
        assertThat(traversal, containsString("## Group Step"));
        assertThat(traversal, containsString("[Read more](group-step.md)"));
        // ...without pulling in the steps' bodies, which live on their own pages.
        assertThat(traversal, not(containsString("fold body")));
        assertThat(traversal, not(containsString("group body")));
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

    @Test
    public void everyPagePointsAtItsOwnVersionsIndex() {
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference") + "\nintro\n\n"
                + summarized("graphson", 1, "GraphSON", "The GraphSON format.") + "\nbody\n";
        final List<MarkdownSplitter.Page> pages = new MarkdownSplitter(MarkdownSplitter.DEFAULT_BUDGET, "3.7.7")
                .split(md, "index.md");
        // Both the index page and the split-off page carry the directive, and it names 3.7.7 rather
        // than resolving to whichever version published to the site root last.
        for (final MarkdownSplitter.Page p : pages) {
            assertThat(p.getContent(), containsString(
                    "> For the complete documentation index, see "
                            + "[llms.txt](https://tinkerpop.apache.org/docs/3.7.7/llms.txt)"));
            assertThat(p.getContent(), not(containsString("[llms.txt](/llms.txt)")));
        }
    }

    @Test
    public void pointerFallsBackToSiteRootWithoutAVersion() {
        assertThat(MarkdownSplitter.llmsPointer(null), containsString("[llms.txt](/llms.txt)"));
        assertThat(MarkdownSplitter.llmsPointer(""), containsString("[llms.txt](/llms.txt)"));
    }

    private static final String SITE = "https://tinkerpop.apache.org/docs/3.7.7/";

    /** Writes a rendered book at {@code <root>/<path>/index.md}. */
    private Path book(final Path root, final String path, final String markdown) throws IOException {
        final Path file = root.resolve(path).resolve("index.md");
        Files.createDirectories(file.getParent());
        Files.write(file, markdown.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static String read(final Path root, final String path) throws IOException {
        return new String(Files.readAllBytes(root.resolve(path)), StandardCharsets.UTF_8);
    }

    /** A reference book with an addE() page, and a provider book whose page links into it. */
    private List<String> splitReferenceAndProvider(final Path root, final String providerBody) throws IOException {
        final Path reference = book(root, "reference", "preamble\n\n" + heading("ref", 1, "Reference")
                + "\nintro\n\n" + summarized("addedge-step", 1, "AddEdge Step", "addE().") + "\nbody\n");
        final Path provider = book(root, "dev/provider", "preamble\n\n" + heading("prov", 1, "Provider")
                + "\nintro\n\n" + summarized("adde-step", 1, "addE()", "addE semantics.") + "\n"
                + providerBody + "\n");
        final List<String> violations = new ArrayList<>();
        final List<Path> books = new ArrayList<>();
        books.add(reference);
        books.add(provider);
        new MarkdownSplitter(MarkdownSplitter.DEFAULT_BUDGET, "3.7.7").splitBooks(root, books, violations);
        return violations;
    }

    @Test
    public void linkToAnotherBookBecomesRelativeLinkToOwningPage() throws IOException {
        final Path root = tmp.getRoot().toPath();
        final List<String> violations = splitReferenceAndProvider(root,
                "See [addE()](" + SITE + "reference/#addedge-step) and [again](" + SITE + "reference#addedge-step).");
        assertThat(violations, is(empty()));
        final String page = read(root, "dev/provider/adde-step.md");
        assertThat(page, containsString("[addE()](../../reference/addedge-step.md#addedge-step)"));
        assertThat(page, containsString("[again](../../reference/addedge-step.md#addedge-step)"));
    }

    @Test
    public void linksPinnedToAnotherVersionOrToBooksOutsideTheRunAreLeftAlone() throws IOException {
        final Path root = tmp.getRoot().toPath();
        final String pinned = "[old](https://tinkerpop.apache.org/docs/3.2.10/reference/#_configuring_2)";
        final String elsewhere = "[recipe](" + SITE + "recipes/#has-traversal)";
        final List<String> violations = splitReferenceAndProvider(root, pinned + " " + elsewhere);
        assertThat(violations, is(empty()));
        final String page = read(root, "dev/provider/adde-step.md");
        assertThat(page, containsString(pinned));
        assertThat(page, containsString(elsewhere));
    }

    @Test
    public void linkToAnchorNoPageInTheOtherBookHasIsAViolation() throws IOException {
        final Path root = tmp.getRoot().toPath();
        final String broken = "[gone](" + SITE + "reference/#mergev-step)";
        final List<String> violations = splitReferenceAndProvider(root, broken);
        assertThat(violations, hasSize(1));
        assertThat(violations.get(0), containsString("reference/#mergev-step"));
        assertThat(read(root, "dev/provider/adde-step.md"), containsString(broken));
    }

    @Test
    public void linkToAnchorNoPageInTheSameBookHasIsAViolation() throws IOException {
        final Path root = tmp.getRoot().toPath();
        final List<String> violations = splitReferenceAndProvider(root, "See [nothing](#Equality).");
        assertThat(violations, hasSize(1));
        assertThat(violations.get(0), containsString("#Equality"));
    }

    @Test
    public void anchorInsideALineIsALinkTarget() {
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference") + "\nintro\n\n"
                + summarized("graphml", 1, "GraphML", "GraphML.") + "\nText <a id=\"inline\"></a>here.\n\n"
                + summarized("graphson", 1, "GraphSON", "GraphSON.") + "\nSee [it](#inline).\n";
        final MarkdownSplitter.SplitBook book = new MarkdownSplitter().splitBook(md, "index.md", null);
        assertThat(page(book.pages, "graphson.md").getContent(), containsString("[it](graphml.md#inline)"));
    }

    @Test
    public void blockAnchorBeforeAHeadingAnchorIsKept() {
        final String md = "preamble\n\n" + heading("io", 1, "IO Reference") + "\nintro\n\n"
                + "<a id=\"graph-step\"></a>\n" + summarized("v-step", 1, "V Step", "V().") + "\nbody\n\n"
                + summarized("other", 1, "Other", "Other.") + "\nSee [graph](#graph-step) and [V](#v-step).\n";
        final MarkdownSplitter.SplitBook book = new MarkdownSplitter().splitBook(md, "index.md", null);
        assertThat(fileNames(book.pages), hasItem("v-step.md"));
        final String other = page(book.pages, "other.md").getContent();
        assertThat(other, containsString("[V](v-step.md#v-step)"));
        assertThat(book.anchorToFile.containsKey("graph-step"), is(true));
        assertThat(other, containsString("[graph](" + book.anchorToFile.get("graph-step") + "#graph-step)"));
    }

    @Test
    public void relativeLinkBetweenBooks() {
        assertThat(MarkdownSplitter.relativeLink("dev/provider", "reference", "a.md"), is("../../reference/a.md"));
        assertThat(MarkdownSplitter.relativeLink("reference", "dev/provider", "b.md"), is("../dev/provider/b.md"));
        assertThat(MarkdownSplitter.relativeLink("tutorials/getting-started", "tutorials/the-gremlin-console",
                "index.md"), is("../the-gremlin-console/index.md"));
    }
}
