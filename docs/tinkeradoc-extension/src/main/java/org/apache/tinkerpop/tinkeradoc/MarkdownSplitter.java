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

    /** Default per-page budget in bytes (~50 KB), per the agent-docs spec's page-size check. */
    static final int DEFAULT_BUDGET = 50_000;

    /** Safety margin (bytes) held back from the packing budget to absorb estimate/render drift. */
    private static final int PACK_SAFETY_MARGIN = 512;

    private static final Pattern ANCHOR = Pattern.compile("^<a id=\"([^\"]+)\"></a>$");
    private static final Pattern HEADING = Pattern.compile("^(#{1,6}) +(.*)$");
    // Intra-document links: [label](#anchor). Capture label and anchor separately.
    private static final Pattern INTRA_LINK = Pattern.compile("\\]\\(#([^)]+)\\)");

    private static final Logger LOG = Logger.getLogger(MarkdownSplitter.class.getName());

    private final int budget;
    private final int packBudget;

    MarkdownSplitter() {
        this(DEFAULT_BUDGET);
    }

    MarkdownSplitter(final int budget) {
        this.budget = budget;
        // Reserve room in the packing budget for (a) the llms.txt pointer prepended to every page
        // after packing, and (b) a small safety margin. The per-node byte estimate can drift a few
        // bytes from the concatenated render (newline/preamble joins), and pages pack right up to
        // the limit; the margin keeps the final UTF-8 page comfortably under the hard cap.
        final int pointer = LLMS_POINTER.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        this.packBudget = Math.max(1, budget - pointer - PACK_SAFETY_MARGIN);
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
            if (bytes.length > budget) {
                LOG.warning("Markdown page " + out + " is " + bytes.length
                        + " bytes, over the " + budget + "-byte budget (indivisible single-heading section)");
            }
        }
        LOG.info("Split " + bookFile + " into " + written.size() + " pages: " + written);
        return written;
    }

    /**
     * CLI entry point: {@code MarkdownSplitter [--budget N] <book.md> [<book.md> ...]}. Each named
     * rendered book file is split in place into agent-sized pages in its own directory.
     */
    public static void main(final String[] args) throws IOException {
        int budget = DEFAULT_BUDGET;
        final List<String> files = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if ("--budget".equals(args[i]) && i + 1 < args.length) {
                budget = Integer.parseInt(args[++i]);
            } else {
                files.add(args[i]);
            }
        }
        if (files.isEmpty()) {
            System.err.println("usage: MarkdownSplitter [--budget N] <book.md> [<book.md> ...]");
            System.exit(2);
        }
        final MarkdownSplitter splitter = new MarkdownSplitter(budget);
        for (final String f : files) {
            final Path p = Path.of(f);
            if (Files.isRegularFile(p)) {
                splitter.splitFile(p);
            } else {
                System.err.println("skip (not a file): " + f);
            }
        }
    }

    /** A rendered page: its file name (without directory), and its Markdown body. */
    static final class Page {
        final String fileName;
        final String content;

        Page(final String fileName, final String content) {
            this.fileName = fileName;
            this.content = content;
        }

        String getFileName() {
            return fileName;
        }

        String getContent() {
            return content;
        }
    }

    /** A parsed heading unit: its level, anchor id (may be null), and the raw block of lines. */
    private static final class Node {
        final int level;
        final String anchor;
        final List<String> lines = new ArrayList<>();
        final List<Node> children = new ArrayList<>();
        int byteSize; // size of this node's own lines plus descendants, computed lazily

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
        computeSizes(root);

        // Decide which nodes start their own page. The root's own lines (preamble) always live on
        // the index page. Each top-level child is placed on the index page if it fits; otherwise it
        // (or its children, recursively) become separate pages.
        final Map<String, String> anchorToFile = new LinkedHashMap<>();
        final List<PagePlan> plans = new ArrayList<>();
        final PagePlan index = new PagePlan(indexFileName);
        plans.add(index);

        // Record every anchor's home page as we assign, so links can be rewritten afterward.
        assignRootAnchors(root, index, anchorToFile);
        final PageCursor cursor = new PageCursor(index);
        cursor.used = preambleSize(root);
        planChildren(root, cursor, plans, anchorToFile);

        // Render each planned page, rewrite cross-page links, and prepend the llms.txt pointer.
        final List<Page> pages = new ArrayList<>();
        for (final PagePlan plan : plans) {
            final String body = plan.render();
            final String rewritten = rewriteLinks(body, plan.fileName, anchorToFile);
            pages.add(new Page(plan.fileName, LLMS_POINTER + rewritten));
        }
        return pages;
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

    private int computeSizes(final Node node) {
        int size = ownSize(node);
        for (final Node c : node.children) size += computeSizes(c);
        node.byteSize = size;
        return size;
    }

    /**
     * The size of a node's own lines (heading + leading content) in UTF-8 bytes, excluding children.
     * Uses UTF-8 byte length (not {@code String.length()}, which counts UTF-16 chars) because pages
     * are written and size-checked as UTF-8; docs contain multi-byte characters (™, →, é, …), so a
     * char count would under-estimate and let a page slip over the byte budget.
     */
    private static int ownSize(final Node node) {
        int size = 0;
        for (final String l : node.lines) {
            size += l.getBytes(java.nio.charset.StandardCharsets.UTF_8).length + 1;
        }
        return size;
    }

    /** The byte size of the document preamble (root's own lines) that always leads the index page. */
    private static int preambleSize(final Node root) {
        return ownSize(root);
    }

    // ---- page planning -----------------------------------------------------

    private static final class PagePlan {
        final String fileName;
        final List<Node> nodes = new ArrayList<>();
        final List<String> preambleLines = new ArrayList<>();

        PagePlan(final String fileName) {
            this.fileName = fileName;
        }

        String render() {
            final StringBuilder sb = new StringBuilder();
            for (final String l : preambleLines) sb.append(l).append('\n');
            for (final Node n : nodes) renderNode(n, sb);
            return sb.toString();
        }

        private void renderNode(final Node n, final StringBuilder sb) {
            for (final String l : n.lines) sb.append(l).append('\n');
            for (final Node c : n.children) renderNode(c, sb);
        }
    }

    /** The root's own (pre-heading) lines and any anchors on them belong to the index page. */
    private void assignRootAnchors(final Node root, final PagePlan index,
                                   final Map<String, String> anchorToFile) {
        index.preambleLines.addAll(root.lines);
        // Anchors that appear in the preamble lines resolve to the index page.
        for (final String line : root.lines) {
            final Matcher am = ANCHOR.matcher(line);
            if (am.matches()) anchorToFile.put(am.group(1), index.fileName);
        }
    }

    /** Mutable pointer to the page currently being filled, plus its running byte size. */
    private static final class PageCursor {
        PagePlan page;
        int used;

        PageCursor(final PagePlan page) {
            this.page = page;
        }
    }

    /**
     * Greedily packs each child of {@code parent} onto the current page, keeping pages as full as
     * possible under the budget while never cutting mid-section:
     * <ul>
     *   <li>If the child's whole subtree fits in the current page's remaining room, place it there.</li>
     *   <li>Else, if the whole subtree fits an empty page, start a fresh page for it.</li>
     *   <li>Else (the subtree alone exceeds the budget), open a page led by the child's own heading
     *       and descend a level, recursively planning its children.</li>
     * </ul>
     * The descend case is what turns an over-budget chapter into per-section pages.
     */
    private void planChildren(final Node parent, final PageCursor cursor,
                              final List<PagePlan> plans, final Map<String, String> anchorToFile) {
        for (final Node child : parent.children) {
            if (cursor.used + child.byteSize <= packBudget) {
                placeWhole(child, cursor.page, anchorToFile);
                cursor.used += child.byteSize;
            } else if (child.byteSize <= packBudget) {
                // Fits a page of its own: start a fresh page and move the cursor there.
                final PagePlan page = newPage(child, plans);
                placeWhole(child, page, anchorToFile);
                cursor.page = page;
                cursor.used = child.byteSize;
            } else {
                // Too big for any single page: lead a new page with the child's own heading, then
                // descend into its children on that same page (they will overflow to more pages).
                final PagePlan page = newPage(child, plans);
                page.nodes.add(headOnly(child, page, anchorToFile));
                final PageCursor childCursor = new PageCursor(page);
                childCursor.used = ownSize(child);
                planChildren(child, childCursor, plans, anchorToFile);
                // Continue the outer loop on the last page the descent produced, so a small sibling
                // after a big chapter can still share that page's leftover room.
                cursor.page = childCursor.page;
                cursor.used = childCursor.used;
            }
        }
    }

    private PagePlan newPage(final Node node, final List<PagePlan> plans) {
        final PagePlan page = new PagePlan(fileNameFor(node, plans));
        plans.add(page);
        return page;
    }

    /** Places a node (and its whole subtree) onto a page, recording all its anchors' home. */
    private void placeWhole(final Node node, final PagePlan page, final Map<String, String> anchorToFile) {
        page.nodes.add(node);
        recordAnchors(node, page.fileName, anchorToFile);
    }

    /**
     * Returns a shallow copy of {@code node} holding only its own lines (heading + leading content),
     * with its children dropped, so the node's heading leads its own page while its subsections are
     * planned separately. Records the node's own anchor on the given page.
     */
    private Node headOnly(final Node node, final PagePlan page, final Map<String, String> anchorToFile) {
        final Node head = new Node(node.level, node.anchor);
        head.lines.addAll(node.lines);
        if (node.anchor != null) anchorToFile.put(node.anchor, page.fileName);
        return head;
    }

    private void recordAnchors(final Node node, final String fileName,
                               final Map<String, String> anchorToFile) {
        if (node.anchor != null) anchorToFile.put(node.anchor, fileName);
        for (final Node c : node.children) recordAnchors(c, fileName, anchorToFile);
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
