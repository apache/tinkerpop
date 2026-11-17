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
import java.util.IdentityHashMap;
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
 * <p>
 * <b>Links between books.</b> The AsciiDoc source links from one book to another with absolute site
 * URLs such as {@code https://tinkerpop.apache.org/docs/x.y.z/reference/#addedge-step}. They resolve
 * in the HTML, where each book is a single page, but in the Markdown the book is split into many
 * pages, so the URL names no page. When the books are split together under a common root (see
 * {@link #splitBooks}), such a link to a book in the same run and the same version is rewritten to a
 * relative link to the page that owns the anchor. Links pinned to another version, as the upgrade
 * docs do, are left alone.
 * <p>
 * <b>Broken links.</b> A link to an anchor that no page owns, within the book or in another book of
 * the run, is reported as a violation alongside the size lint, so {@code --strict} fails the build.
 */
class MarkdownSplitter {

    /**
     * Per-page size budget in bytes (~50 KB), per the agent-docs spec's page-size check. Splitting is
     * summary-driven, not size-driven; this budget is only used by the size lint to flag pages that
     * exceed it and are not marked {@code allow-oversize}.
     */
    static final int DEFAULT_BUDGET = 50_000;

    private static final Pattern ANCHOR = Pattern.compile("^<a id=\"([^\"]+)\"></a>$");
    // Any anchor, including one inside a line (an inline anchor:id[] in the AsciiDoc).
    private static final Pattern ANY_ANCHOR = Pattern.compile("<a id=\"([^\"]+)\"></a>");
    private static final Pattern HEADING = Pattern.compile("^(#{1,6}) +(.*)$");
    // Intra-document links: [label](#anchor). Capture label and anchor separately.
    private static final Pattern INTRA_LINK = Pattern.compile("\\]\\(#([^)]+)\\)");
    // The sole page-break signal: the hidden marker MarkdownConverter emits from [llms-summary="..."].
    private static final Pattern SUMMARY_MARKER = Pattern.compile("^<!-- llms-summary: .* -->$");
    // The same marker, capturing the summary text for rendering into parent table-of-contents entries.
    private static final Pattern SUMMARY_TEXT = Pattern.compile("^<!-- llms-summary: (.*) -->$");
    // Marker MarkdownConverter emits from allow-oversize="true": this page may exceed the budget.
    private static final String ALLOW_OVERSIZE_MARKER = "<!-- llms-allow-oversize -->";

    private static final Logger LOG = Logger.getLogger(MarkdownSplitter.class.getName());

    private final int budget;
    private final String version;
    private final String llmsPointer;
    /** Links between books for {@link #version}, or {@code null} without a version. */
    private final Pattern crossBookLink;

    MarkdownSplitter() {
        this(DEFAULT_BUDGET);
    }

    MarkdownSplitter(final int budget) {
        this(budget, null);
    }

    /**
     * @param version the TinkerPop version these pages are published under, used to build the
     *                per-page index pointer. When {@code null} the pointer falls back to the
     *                site-root {@code /llms.txt}, which is not version-correct.
     */
    MarkdownSplitter(final int budget, final String version) {
        this.budget = budget;
        this.version = version == null || version.isEmpty() ? null : version;
        this.llmsPointer = llmsPointer(version);
        this.crossBookLink = this.version == null ? null : crossBookLink(this.version);
        if (version == null || version.isEmpty()) {
            LOG.warning("No version supplied; pages will point at the site-root /llms.txt rather "
                    + "than this version's index. Pass --version to make the pointer version-correct.");
        }
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
        return splitBooks(null, List.of(bookFile), violations).get(0);
    }

    /**
     * Splits several rendered books in place, as {@link #splitFile(Path, List)} does for one, and
     * resolves the links between them. With a {@code root}, each book is known by its directory
     * relative to it ({@code reference}, {@code dev/provider}), which is also its path on the site,
     * so an absolute link to {@code https://tinkerpop.apache.org/docs/<version>/<book>/#anchor} for
     * this splitter's version is rewritten to a relative link to the page owning the anchor. Without
     * a root, or without a version, links between books are left as they are.
     * <p>
     * Every link to an anchor no page owns, in its own book or in another book of the run, is added
     * to {@code violations}, as is every page over the byte budget.
     *
     * @return the file names written into each book's directory, in the order of {@code bookFiles}
     */
    List<List<String>> splitBooks(final Path root, final List<Path> bookFiles,
                                  final List<String> violations) throws IOException {
        final List<SplitBook> books = new ArrayList<>();
        final Map<String, SplitBook> byPath = new LinkedHashMap<>();
        for (final Path bookFile : bookFiles) {
            final String markdown = new String(Files.readAllBytes(bookFile), StandardCharsets.UTF_8);
            final String path = root == null ? null : bookPath(root, bookFile.getParent());
            final SplitBook book = splitBook(markdown, bookFile.getFileName().toString(), path);
            books.add(book);
            if (path != null) byPath.put(path, book);
        }

        final List<List<String>> written = new ArrayList<>();
        for (int i = 0; i < books.size(); i++) {
            final Path bookFile = bookFiles.get(i);
            final SplitBook book = books.get(i);
            final Path dir = bookFile.getParent();
            final List<String> names = new ArrayList<>();
            for (final Page split : book.pages) {
                final Path out = dir.resolve(split.getFileName());
                final Page page = resolveLinks(book, split, byPath, out, violations);
                final byte[] bytes = page.getContent().getBytes(StandardCharsets.UTF_8);
                Files.write(out, bytes);
                names.add(page.getFileName());
                if (bytes.length > budget && !page.isOversizeAllowed()) {
                    violations.add(out + " is " + bytes.length + " bytes, over the " + budget
                            + "-byte budget; add an llms-summary to a subsection to split it, "
                            + "or mark the section allow-oversize=\"true\" if it must stay whole");
                }
            }
            LOG.info("Split " + bookFile + " into " + names.size() + " pages: " + names);
            written.add(names);
        }
        return written;
    }

    /** A book's directory relative to the docs root, with {@code /} separators. */
    private static String bookPath(final Path root, final Path bookDir) {
        final Path rel = root.toAbsolutePath().normalize().relativize(bookDir.toAbsolutePath().normalize());
        return rel.toString().replace('\\', '/');
    }

    /**
     * CLI entry point: {@code MarkdownSplitter [--budget N] [--strict] [--version x.y.z]
     * [--root dir] <book.md> [<book.md> ...]}.
     * Each named rendered book file is split in place (summary-driven) into pages in its own
     * directory. Pages over the byte budget that are not flagged {@code allow-oversize}, and links to
     * anchors no page owns, are reported; with {@code --strict} the process exits non-zero when any
     * such violation exists, so the docs build can gate on it. {@code --version} sets the version the
     * per-page index pointer resolves to. {@code --root} is the docs root the books sit under, which
     * turns on rewriting of links between books (see {@link #splitBooks}).
     */
    public static void main(final String[] args) throws IOException {
        int budget = DEFAULT_BUDGET;
        boolean strict = false;
        String version = null;
        Path root = null;
        final List<String> files = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if ("--budget".equals(args[i]) && i + 1 < args.length) {
                budget = Integer.parseInt(args[++i]);
            } else if ("--version".equals(args[i]) && i + 1 < args.length) {
                version = args[++i];
            } else if ("--root".equals(args[i]) && i + 1 < args.length) {
                root = Path.of(args[++i]);
            } else if ("--strict".equals(args[i])) {
                strict = true;
            } else {
                files.add(args[i]);
            }
        }
        if (files.isEmpty()) {
            System.err.println("usage: MarkdownSplitter [--budget N] [--strict] [--version x.y.z] "
                    + "[--root dir] <book.md> [<book.md> ...]");
            System.exit(2);
        }
        final MarkdownSplitter splitter = new MarkdownSplitter(budget, version);
        final List<String> violations = new ArrayList<>();
        final List<Path> books = new ArrayList<>();
        for (final String f : files) {
            final Path p = Path.of(f);
            if (Files.isRegularFile(p)) {
                books.add(p);
            } else {
                System.err.println("skip (not a file): " + f);
            }
        }
        splitter.splitBooks(root, books, violations);
        if (!violations.isEmpty()) {
            System.err.println("\nMarkdown page-size and link violations (" + violations.size() + "):");
            for (final String v : violations) System.err.println("  - " + v);
            if (strict) {
                System.err.println("\nFailing due to --strict. Fix the offending sections or links and re-run.");
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
     * their own pages named from their anchor id. Every page starts with the llms.txt pointer and,
     * when the book begins with {@link MarkdownConverter#LICENSE_COMMENT}, the license comment.
     *
     * @param markdown      the full rendered Markdown book
     * @param indexFileName the file name for the landing page (e.g. {@code index.md})
     * @return the ordered list of pages (always at least one)
     */
    List<Page> split(final String markdown, final String indexFileName) {
        return splitBook(markdown, indexFileName, null).pages;
    }

    /** One book split into pages, with links inside the book already rewritten. */
    static final class SplitBook {
        /** The book's directory relative to the docs root, or {@code null} if not known. */
        final String path;
        final List<Page> pages;
        /** Which page each anchor in the book lands on. */
        final Map<String, String> anchorToFile;

        SplitBook(final String path, final List<Page> pages, final Map<String, String> anchorToFile) {
            this.path = path;
            this.pages = pages;
            this.anchorToFile = anchorToFile;
        }
    }

    SplitBook splitBook(final String markdown, final String indexFileName, final String path) {
        // The license comment MarkdownConverter puts at the top of the book belongs on every page,
        // not only the landing page that would otherwise inherit the book's preamble.
        final boolean licensed = markdown.startsWith(MarkdownConverter.LICENSE_COMMENT);
        final String header = llmsPointer + (licensed ? MarkdownConverter.LICENSE_COMMENT : "");
        final Node root = parse(licensed
                ? markdown.substring(MarkdownConverter.LICENSE_COMMENT.length())
                : markdown);

        // Summary-driven splitting: a section becomes its own page if and only if it carries an
        // llms-summary (surfaced as the <!-- llms-summary: ... --> marker). The document root is
        // always a page (the index). A page holds its owning node's content plus every descendant
        // up to — but not including — the next summarized descendant, which breaks off to its own
        // page. There is no size-based splitting; page size is an author concern surfaced by the
        // build's size lint, with intentional exceptions flagged via allow-oversize.
        // The document title (the single leading top-level section — the book's "= Title") is the
        // landing page: it owns index.md so its own llms-summary describes the index and its content
        // renders there, rather than breaking off to a page of its own. Fold it into the root so the
        // root preamble + doctitle render together as index.md. Everything below still breaks on
        // summaries as usual.
        final Node docTitle = leadingDocTitle(root);
        if (docTitle != null) {
            root.lines.addAll(docTitle.lines);
            root.children.remove(docTitle);
            root.children.addAll(0, docTitle.children);
        }

        final Map<String, String> anchorToFile = new LinkedHashMap<>();
        final Map<Node, String> nodeToFile = new IdentityHashMap<>();
        final List<PagePlan> plans = new ArrayList<>();
        final PagePlan index = new PagePlan(indexFileName, root);
        plans.add(index);
        nodeToFile.put(root, index.fileName);
        recordAnchorsUntilBreak(root, index.fileName, anchorToFile);
        planBreaks(root, plans, anchorToFile, nodeToFile);

        final List<Page> pages = new ArrayList<>();
        for (final PagePlan plan : plans) {
            final StringBuilder body = new StringBuilder();
            renderPage(plan.owner, body, plan == index, nodeToFile);
            final String rewritten = rewriteLinks(body.toString(), plan.fileName, anchorToFile);
            pages.add(new Page(plan.fileName, header + rewritten, isAllowOversize(plan.owner)));
        }
        return new SplitBook(path, pages, anchorToFile);
    }

    /**
     * Returns the document-title node to fold into the index page, or {@code null} if there is none.
     * The doctitle is the book's leading section: the root's first child, when that child is the sole
     * shallowest-level top section (i.e. the other top-level siblings are its heading level or
     * deeper). In practice this is the {@code = Title} that AsciidoctorJ renders as the first h1.
     */
    private static Node leadingDocTitle(final Node root) {
        if (root.children.isEmpty()) return null;
        final Node first = root.children.get(0);
        // Only treat it as the doctitle when it carries content meant to lead the landing page: it
        // has a summary (so it would otherwise wrongly break off) or the root has no preamble of its
        // own. Guard against odd trees by requiring it be the shallowest top-level section.
        for (final Node sib : root.children) {
            if (sib != first && sib.level < first.level) return null;
        }
        return first;
    }

    /**
     * Walks the tree creating a page for every summarized descendant of {@code node} (the node's own
     * page having already been created). Recurses through the whole tree so nested summarized
     * sections each get a page.
     */
    private void planBreaks(final Node node, final List<PagePlan> plans,
                            final Map<String, String> anchorToFile,
                            final Map<Node, String> nodeToFile) {
        for (final Node child : node.children) {
            if (hasSummary(child)) {
                final PagePlan page = new PagePlan(fileNameFor(child, plans), child);
                plans.add(page);
                nodeToFile.put(child, page.fileName);
                recordAnchorsUntilBreak(child, page.fileName, anchorToFile);
            }
            planBreaks(child, plans, anchorToFile, nodeToFile);
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
            final Matcher am = ANY_ANCHOR.matcher(line);
            while (am.find()) anchorToFile.put(am.group(1), fileName);
        }
        for (final Node child : node.children) {
            if (hasSummary(child)) continue; // child owns its own page; recorded when its page is made
            recordAnchorsUntilBreak(child, fileName, anchorToFile);
        }
    }

    /**
     * Renders one page: {@code owner}'s own lines, then each descendant up to the next summarized
     * section. A summarized descendant does not render its content here (that lives on its own page);
     * in its place a table-of-contents reference to that page is emitted, so the parent stays a
     * coherent, navigable outline rather than silently dropping the child's content.
     */
    private void renderPage(final Node owner, final StringBuilder sb, final boolean isIndex,
                            final Map<Node, String> nodeToFile) {
        for (final String l : owner.lines) sb.append(l).append('\n');
        for (final Node child : owner.children) {
            if (hasSummary(child)) {
                appendChildReference(child, sb, nodeToFile);
                continue;
            }
            renderSubtree(child, sb, nodeToFile);
        }
    }

    private void renderSubtree(final Node node, final StringBuilder sb,
                               final Map<Node, String> nodeToFile) {
        for (final String l : node.lines) sb.append(l).append('\n');
        for (final Node child : node.children) {
            if (hasSummary(child)) {
                appendChildReference(child, sb, nodeToFile);
                continue;
            }
            renderSubtree(child, sb, nodeToFile);
        }
    }

    /**
     * Emits a table-of-contents reference to a child section that has broken off to its own page:
     * the child's heading (kept at its original level), its llms-summary as prose, and a link to the
     * page. This keeps the parent a coherent outline that points at every spun-off child, the same
     * way {@code llms.txt} references pages.
     */
    private void appendChildReference(final Node child, final StringBuilder sb,
                                      final Map<Node, String> nodeToFile) {
        sb.append('\n');
        final String heading = headingText(child);
        if (heading != null) {
            sb.append("#".repeat(Math.max(1, child.level))).append(' ').append(heading).append("\n\n");
        }
        final String summary = summaryText(child);
        if (summary != null) {
            sb.append(summary).append("\n\n");
        }
        final String file = nodeToFile.get(child);
        if (file != null) {
            sb.append("[Read more](").append(file).append(")\n\n");
        }
    }

    /** The text of a node's heading line (the part after the {@code #}s), or {@code null} if none. */
    private static String headingText(final Node node) {
        for (final String line : node.lines) {
            final Matcher hm = HEADING.matcher(line);
            if (hm.matches()) return hm.group(2).trim();
        }
        return null;
    }

    /** The llms-summary text carried by a node, or {@code null} if it has none. */
    private static String summaryText(final Node node) {
        for (final String line : node.lines) {
            final Matcher m = SUMMARY_TEXT.matcher(line.trim());
            if (m.matches()) return m.group(1).trim();
        }
        return null;
    }

    /** Canonical base for the versioned documentation tree. */
    private static final String DOCS_BASE_URL = "https://tinkerpop.apache.org/docs";

    /**
     * The agent-facing directive prepended to every page (agentdocsspec.com {@code
     * llms-txt-directive-md} check): a top-of-page blockquote pointing at this version's index.
     * <p>
     * The URL is absolute and version-qualified so a page always points at the index describing that
     * same version, mirroring the HTML backend's directive in {@code docs/src/docinfo-footer.html}.
     * A site-root {@code /llms.txt} would instead resolve to whichever version published last.
     * <p>
     * The version cannot be substituted from the {@code x.y.z} placeholder the way the rest of the
     * Markdown is: {@link MarkdownConverter} does that substitution while rendering, and the splitter
     * runs afterwards as a separate pass over the already-rendered book.
     */
    static String llmsPointer(final String version) {
        final String url = version == null || version.isEmpty()
                ? "/llms.txt"
                : DOCS_BASE_URL + "/" + version + "/llms.txt";
        return "> For the complete documentation index, see [llms.txt](" + url + ")\n\n";
    }

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
                // Two anchor lines in a row: only the second can belong to a heading, so the first
                // (a block's anchor) stays as content.
                if (pendingAnchor != null) stack.peek().lines.add("<a id=\"" + pendingAnchor + "\"></a>");
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

    /**
     * Rewrites a page's links to other books of the run to relative links to the pages owning their
     * anchors, and reports every link, within the book or to another book of the run, whose anchor
     * no page owns.
     */
    private Page resolveLinks(final SplitBook book, final Page page, final Map<String, SplitBook> books,
                              final Path out, final List<String> violations) {
        final String body = page.getContent();
        final Matcher local = INTRA_LINK.matcher(body);
        while (local.find()) {
            if (!book.anchorToFile.containsKey(local.group(1))) {
                violations.add(out + " links to #" + local.group(1) + ", which no page in its book has");
            }
        }
        if (version == null || book.path == null) return page;

        final Matcher m = crossBookLink.matcher(body);
        final StringBuffer sb = new StringBuffer();
        while (m.find()) {
            final String target = m.group(1);
            final String anchor = m.group(2);
            final SplitBook other = books.get(target);
            String replacement = m.group();
            if (other != null) {
                final String file = other.anchorToFile.get(anchor);
                if (file == null) {
                    violations.add(out + " links to " + DOCS_BASE_URL + "/" + version + "/" + target
                            + "/#" + anchor + ", which no page in that book has");
                } else {
                    replacement = "](" + relativeLink(book.path, other.path, file) + "#" + anchor + ")";
                }
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return new Page(page.getFileName(), sb.toString(), page.isOversizeAllowed());
    }

    /**
     * An absolute link into this version's docs with an anchor and no page:
     * {@code ](https://tinkerpop.apache.org/docs/<version>/<book>/#anchor)}, the trailing slash after
     * the book optional. Captures the book path and the anchor.
     */
    private static Pattern crossBookLink(final String version) {
        return Pattern.compile("\\]\\(" + Pattern.quote(DOCS_BASE_URL + "/" + version + "/")
                + "([^)#\\s]*?)/?#([^)\\s]+)\\)");
    }

    /** The relative link from a page in book {@code from} to {@code file} in book {@code to}. */
    static String relativeLink(final String from, final String to, final String file) {
        return Path.of(from).relativize(Path.of(to, file)).toString().replace('\\', '/');
    }
}
