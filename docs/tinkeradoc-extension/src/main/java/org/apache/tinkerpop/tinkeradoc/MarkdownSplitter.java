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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Splits a single rendered Markdown book (as produced by {@link MarkdownConverter}) into
 * agent-sized pages, each a named, heading-bounded section under the size budget, and rewrites
 * intra-document anchor links to point across pages.
 * <p>
 * <b>Splitting rule (size-driven, heading-aligned).</b> Emit each page at the coarsest heading
 * level that keeps it under the byte budget; descend a level (h2 → h3 → h4) only for subtrees that
 * exceed the budget. Every page is a whole heading-bounded section, so page URLs stay semantic and
 * stable. This is one uniform rule with no per-file special cases: small books stay as whole-h2
 * pages, large books descend where needed.
 * <p>
 * The splitter operates purely on the rendered Markdown text. It relies on the
 * {@code <a id="..."></a>} anchor {@link MarkdownConverter} emits immediately before each heading:
 * the anchor id becomes both the page's file name (for pages that start a file) and the link
 * fragment. Links of the form {@code [label](#anchor)} are rewritten to {@code [label](file.md#anchor)}
 * when the target anchor lives on a different page.
 */
class MarkdownSplitter {

    /**
     * Per-page size budget in bytes (~50 KB), per the agent-docs spec's page-size check. Splitting is
     * summary-driven, not size-driven; this budget is only used by the size lint to flag pages that
     * exceed it and are not marked {@code allow-oversize}.
     */
    static final int DEFAULT_BUDGET = 50_000;

    private static final Pattern ANCHOR = Pattern.compile("^<a id=\"([^\"]+)\"></a>$");
    private static final Pattern HEADING = Pattern.compile("^(#{1,6}) +(.*)$");
    // Intra-document links: [label](#anchor). Capture label and anchor separately.
    private static final Pattern INTRA_LINK = Pattern.compile("\\]\\(#([^)]+)\\)");
    // The sole page-break signal: the hidden marker MarkdownConverter emits from [llms-summary="..."].
    private static final Pattern SUMMARY_MARKER = Pattern.compile("^<!-- llms-summary: .* -->$");
    // Marker MarkdownConverter emits from allow-oversize="true": this page may exceed the budget.
    private static final String ALLOW_OVERSIZE_MARKER = "<!-- llms-allow-oversize -->";

    private static final Logger LOG = Logger.getLogger(MarkdownSplitter.class.getName());

    private final int budget;

    MarkdownSplitter() {
        this(DEFAULT_BUDGET);
    }

    MarkdownSplitter(final int budget) {
        this.budget = budget;
    }

    /**
     * Splits a rendered book file in place: reads {@code bookFile} (e.g. {@code dev/io/index.md}),
     * writes the resulting pages into its parent directory, and replaces the original with the
     * landing page. Pages that still exceed the budget (indivisible single-heading leaves) are
     * reported so silent truncation never masquerades as full coverage.
     *
     * @param bookFile the rendered single-file book to split
     * @return the file names written into the book's directory
     */
    List<String> splitFile(final Path bookFile) throws IOException {
        return splitFile(bookFile, new ArrayList<>());
    }

    /**
     * Splits {@code bookFile} in place and appends a human-readable message to {@code violations} for
     * every written page that exceeds the byte budget and is not flagged {@code allow-oversize}.
     * Splitting is summary-driven; the budget is enforced only as a lint (the caller decides whether
     * violations fail the build).
     *
     * @return the file names written into the book's directory
     */
    List<String> splitFile(final Path bookFile, final List<String> violations) throws IOException {
        final String markdown = new String(Files.readAllBytes(bookFile), StandardCharsets.UTF_8);
        final Path dir = bookFile.getParent();
        final String indexName = bookFile.getFileName().toString();
        final List<Page> pages = split(markdown, indexName);

        final List<String> written = new ArrayList<>();
        for (final Page page : pages) {
            final Path out = dir.resolve(page.getFileName());
            final byte[] bytes = page.getContent().getBytes(StandardCharsets.UTF_8);
            Files.write(out, bytes);
            written.add(page.getFileName());
            if (bytes.length > budget && !page.isOversizeAllowed()) {
                violations.add(out + " is " + bytes.length + " bytes, over the " + budget
                        + "-byte budget; add an llms-summary to a subsection to split it, "
                        + "or mark the section allow-oversize=\"true\" if it must stay whole");
            }
        }
        LOG.info("Split " + bookFile + " into " + written.size() + " pages: " + written);
        return written;
    }

    /**
     * CLI entry point: {@code MarkdownSplitter [--budget N] [--strict] <book.md> [<book.md> ...]}.
     * Each named rendered book file is split in place (summary-driven) into pages in its own
     * directory. Pages over the byte budget that are not flagged {@code allow-oversize} are reported;
     * with {@code --strict} the process exits non-zero when any such violation exists, so the docs
     * build can gate on it.
     */
    public static void main(final String[] args) throws IOException {
        int budget = DEFAULT_BUDGET;
        boolean strict = false;
        final List<String> files = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if ("--budget".equals(args[i]) && i + 1 < args.length) {
                budget = Integer.parseInt(args[++i]);
            } else if ("--strict".equals(args[i])) {
                strict = true;
            } else {
                files.add(args[i]);
            }
        }
        if (files.isEmpty()) {
            System.err.println("usage: MarkdownSplitter [--budget N] [--strict] <book.md> [<book.md> ...]");
            System.exit(2);
        }
        final MarkdownSplitter splitter = new MarkdownSplitter(budget);
        final List<String> violations = new ArrayList<>();
        for (final String f : files) {
            final Path p = Path.of(f);
            if (Files.isRegularFile(p)) {
                splitter.splitFile(p, violations);
            } else {
                System.err.println("skip (not a file): " + f);
            }
        }
        if (!violations.isEmpty()) {
            System.err.println("\nMarkdown page-size budget violations (" + violations.size() + "):");
            for (final String v : violations) System.err.println("  - " + v);
            if (strict) {
                System.err.println("\nFailing due to --strict. Curate the offending sections and re-run.");
                System.exit(1);
            }
        }
    }

    /** A rendered page: its file name (without directory), its Markdown body, and its oversize flag. */
    static final class Page {
        final String fileName;
        final String content;
        final boolean oversizeAllowed;

        Page(final String fileName, final String content, final boolean oversizeAllowed) {
            this.fileName = fileName;
            this.content = content;
            this.oversizeAllowed = oversizeAllowed;
        }

        String getFileName() {
            return fileName;
        }

        String getContent() {
            return content;
        }

        boolean isOversizeAllowed() {
            return oversizeAllowed;
        }
    }

    /** A parsed heading unit: its level, anchor id (may be null), and the raw block of lines. */
    private static final class Node {
        final int level;
        final String anchor;
        final List<String> lines = new ArrayList<>();
        final List<Node> children = new ArrayList<>();

        Node(final int level, final String anchor) {
            this.level = level;
            this.anchor = anchor;
        }
    }

    /**
     * Splits {@code markdown} into pages. The first page always carries the file name
     * {@code indexFileName} (typically {@code index.md}) and holds the document preamble plus the
     * top-level structure that fits; deeper sections that individually exceed the budget become
     * their own pages named from their anchor id.
     *
     * @param markdown      the full rendered Markdown book
     * @param indexFileName the file name for the landing page (e.g. {@code index.md})
     * @return the ordered list of pages (always at least one)
     */
    List<Page> split(final String markdown, final String indexFileName) {
        final Node root = parse(markdown);

        // Summary-driven splitting: a section becomes its own page if and only if it carries an
        // llms-summary (surfaced as the <!-- llms-summary: ... --> marker). The document root is
        // always a page (the index). A page holds its owning node's content plus every descendant
        // up to — but not including — the next summarized descendant, which breaks off to its own
        // page. There is no size-based splitting; page size is an author concern surfaced by the
        // build's size lint, with intentional exceptions flagged via allow-oversize.
        final Map<String, String> anchorToFile = new LinkedHashMap<>();
        final List<PagePlan> plans = new ArrayList<>();
        final PagePlan index = new PagePlan(indexFileName, root);
        plans.add(index);
        recordAnchorsUntilBreak(root, index.fileName, anchorToFile);
        planBreaks(root, plans, anchorToFile);

        final List<Page> pages = new ArrayList<>();
        for (final PagePlan plan : plans) {
            final StringBuilder body = new StringBuilder();
            renderPage(plan.owner, body, plan == index);
            final String rewritten = rewriteLinks(body.toString(), plan.fileName, anchorToFile);
            pages.add(new Page(plan.fileName, LLMS_POINTER + rewritten, isAllowOversize(plan.owner)));
        }
        return pages;
    }

    /**
     * Walks the tree creating a page for every summarized descendant of {@code node} (the node's own
     * page having already been created). Recurses through the whole tree so nested summarized
     * sections each get a page.
     */
    private void planBreaks(final Node node, final List<PagePlan> plans,
                            final Map<String, String> anchorToFile) {
        for (final Node child : node.children) {
            if (hasSummary(child)) {
                final PagePlan page = new PagePlan(fileNameFor(child, plans), child);
                plans.add(page);
                recordAnchorsUntilBreak(child, page.fileName, anchorToFile);
            }
            planBreaks(child, plans, anchorToFile);
        }
    }

    /**
     * Records that {@code node} and all descendants that render on the same page (i.e. everything
     * down to the next summarized section) resolve to {@code fileName}, so links can be rewritten.
     */
    private void recordAnchorsUntilBreak(final Node node, final String fileName,
                                         final Map<String, String> anchorToFile) {
        if (node.anchor != null) anchorToFile.put(node.anchor, fileName);
        for (final String line : node.lines) {
            final Matcher am = ANCHOR.matcher(line);
            if (am.matches()) anchorToFile.put(am.group(1), fileName);
        }
        for (final Node child : node.children) {
            if (hasSummary(child)) continue; // child owns its own page; recorded when its page is made
            recordAnchorsUntilBreak(child, fileName, anchorToFile);
        }
    }

    /**
     * Renders one page: {@code owner}'s own lines, then each descendant up to the next summarized
     * section. Summarized descendants are omitted here (they render on their own page).
     */
    private void renderPage(final Node owner, final StringBuilder sb, final boolean isIndex) {
        for (final String l : owner.lines) sb.append(l).append('\n');
        for (final Node child : owner.children) {
            if (hasSummary(child)) continue;
            renderSubtree(child, sb);
        }
    }

    private void renderSubtree(final Node node, final StringBuilder sb) {
        for (final String l : node.lines) sb.append(l).append('\n');
        for (final Node child : node.children) {
            if (hasSummary(child)) continue;
            renderSubtree(child, sb);
        }
    }

    /**
     * The agent-facing directive prepended to every page (agentdocsspec.com {@code
     * llms-txt-directive-md} check): a top-of-page blockquote pointing at the site-root index.
     */
    static final String LLMS_POINTER =
            "> For the complete documentation index, see [llms.txt](/llms.txt)\n\n";

    // ---- parsing -----------------------------------------------------------

    private Node parse(final String markdown) {
        final String[] lines = markdown.split("\n", -1);
        final Node root = new Node(0, null);
        final java.util.Deque<Node> stack = new java.util.ArrayDeque<>();
        stack.push(root);

        String pendingAnchor = null;
        boolean inFence = false;
        for (int i = 0; i < lines.length; i++) {
            final String line = lines[i];
            // Track fenced code blocks: a line starting with ``` toggles fence state. Lines inside a
            // fence are always content, never headings — otherwise shell/YAML/properties comment
            // lines like "# Spark Configuration" would be mistaken for section headings and split
            // the page mid-code-block.
            if (line.startsWith("```")) {
                inFence = !inFence;
                if (pendingAnchor != null) {
                    stack.peek().lines.add("<a id=\"" + pendingAnchor + "\"></a>");
                    pendingAnchor = null;
                }
                stack.peek().lines.add(line);
                continue;
            }
            final Matcher am = inFence ? null : ANCHOR.matcher(line);
            if (am != null && am.matches()) {
                pendingAnchor = am.group(1);
                // The anchor line belongs to the heading node it precedes; hold it.
                continue;
            }
            final Matcher hm = inFence ? null : HEADING.matcher(line);
            if (hm != null && hm.matches()) {
                final int level = hm.group(1).length();
                final Node node = new Node(level, pendingAnchor);
                if (pendingAnchor != null) {
                    node.lines.add("<a id=\"" + pendingAnchor + "\"></a>");
                }
                node.lines.add(line);
                pendingAnchor = null;
                // Pop until the top of stack is a strictly shallower node (its parent).
                while (stack.peek().level >= level) stack.pop();
                stack.peek().children.add(node);
                stack.push(node);
            } else {
                // A dangling anchor not followed by a heading: keep it as content.
                if (pendingAnchor != null) {
                    stack.peek().lines.add("<a id=\"" + pendingAnchor + "\"></a>");
                    pendingAnchor = null;
                }
                stack.peek().lines.add(line);
            }
        }
        return root;
    }

    // ---- page planning (summary-driven) ------------------------------------

    /** A planned page: its file name and the node whose subtree (up to the next break) it renders. */
    private static final class PagePlan {
        final String fileName;
        final Node owner;

        PagePlan(final String fileName, final Node owner) {
            this.fileName = fileName;
            this.owner = owner;
        }
    }

    /** Whether a node carries an llms-summary (the sole page-break signal). */
    private static boolean hasSummary(final Node node) {
        for (final String line : node.lines) {
            if (SUMMARY_MARKER.matcher(line.trim()).matches()) return true;
        }
        return false;
    }

    /** Whether a node is flagged to permit exceeding the size budget without a lint failure. */
    static boolean isAllowOversize(final Node node) {
        for (final String line : node.lines) {
            if (line.trim().equals(ALLOW_OVERSIZE_MARKER)) return true;
        }
        return false;
    }

    private String fileNameFor(final Node node, final List<PagePlan> plans) {
        final String base = node.anchor != null && !node.anchor.isEmpty()
                ? sanitize(node.anchor) : "section";
        String candidate = base + ".md";
        int n = 2;
        while (fileExists(plans, candidate)) {
            candidate = base + "-" + (n++) + ".md";
        }
        return candidate;
    }

    private boolean fileExists(final List<PagePlan> plans, final String fileName) {
        for (final PagePlan p : plans) if (p.fileName.equals(fileName)) return true;
        return false;
    }

    private static String sanitize(final String anchor) {
        // Anchor ids are already url-safe (letters, digits, _ and -), but guard against stray chars.
        return anchor.replaceAll("[^A-Za-z0-9_.-]", "-");
    }

    // ---- link rewriting ----------------------------------------------------

    /**
     * Rewrites intra-document links {@code ](#anchor)} on {@code thisFile} so that anchors living on
     * a different page become {@code ](otherFile.md#anchor)}. Anchors on the same page keep their
     * bare {@code #anchor} form. Unknown anchors are left unchanged.
     */
    private String rewriteLinks(final String body, final String thisFile,
                                final Map<String, String> anchorToFile) {
        final Matcher m = INTRA_LINK.matcher(body);
        final StringBuffer sb = new StringBuffer();
        while (m.find()) {
            final String anchor = m.group(1);
            final String home = anchorToFile.get(anchor);
            final String replacement;
            if (home == null || home.equals(thisFile)) {
                replacement = "](#" + anchor + ")";
            } else {
                replacement = "](" + home + "#" + anchor + ")";
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }
}
