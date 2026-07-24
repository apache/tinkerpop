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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Generates an {@code llms.txt} discovery index (per agentdocsspec.com / llmstxt.org) over the split
 * Markdown mirror. The index is a single file with an H1 project name, a summary blockquote, and one
 * H2 section per book listing every page as {@code [title](path): description}. It links every page
 * so coverage is 100% (the spec wants >=95%), and is kept well under the 50,000-character cap.
 */
class LlmsTxtGenerator {

    private static final Logger LOG = Logger.getLogger(LlmsTxtGenerator.class.getName());

    /** Spec size cap for llms.txt: pass under 50,000 characters. */
    static final int SIZE_CAP = 50_000;

    private static final Pattern HEADING = Pattern.compile("^#{1,6} +(.*)$");
    private static final Pattern ANCHOR = Pattern.compile("^<a id=\"[^\"]+\"></a>$");
    private static final Pattern LLMS_POINTER_LINE = Pattern.compile("^> For the complete documentation index.*$");
    // Author-curated summary carried as a hidden comment by MarkdownConverter: <!-- llms-summary: ... -->
    private static final Pattern LLMS_SUMMARY_COMMENT = Pattern.compile("^<!-- llms-summary: (.*) -->$");
    // Strip inline Markdown/HTML noise from a description snippet.
    private static final Pattern INLINE_LINK = Pattern.compile("\\[([^\\]]+)\\]\\([^)]*\\)");
    private static final Pattern INLINE_CODE = Pattern.compile("`([^`]*)`");
    private static final Pattern HTML_TAG = Pattern.compile("<[^>]+>");

    /** One page's index metadata. */
    static final class Entry {
        final String relativePath; // e.g. "dev/io/graphson.md"
        final String title;
        final String description;

        Entry(final String relativePath, final String title, final String description) {
            this.relativePath = relativePath;
            this.title = title;
            this.description = description;
        }
    }

    private final String projectName;
    private final String summary;
    private final String linkPrefix;

    LlmsTxtGenerator(final String projectName, final String summary) {
        this(projectName, summary, "");
    }

    /**
     * @param linkPrefix prepended to every page path in the emitted links. Empty for an index that
     *                   sits in the same directory as the pages (local build); set to e.g.
     *                   {@code docs/3.7.7/} when the index is written to the site root so links
     *                   resolve to the versioned docs tree.
     */
    LlmsTxtGenerator(final String projectName, final String summary, final String linkPrefix) {
        this.projectName = projectName;
        this.summary = summary;
        this.linkPrefix = linkPrefix == null ? "" : linkPrefix;
    }

    /**
     * Renders the llms.txt body from entries grouped by book. Books are the top path segment (e.g.
     * {@code dev/io} -> section "dev/io"); within a section, the book landing page ({@code index.md})
     * is listed first, then the rest in path order.
     */
    String generate(final List<Entry> entries) {
        final StringBuilder sb = new StringBuilder();
        sb.append("# ").append(projectName).append("\n\n");
        sb.append("> ").append(summary).append("\n\n");

        // Group by book (the directory portion of the relative path).
        final Map<String, List<Entry>> byBook = new LinkedHashMap<>();
        for (final Entry e : entries) {
            final String book = bookOf(e.relativePath);
            byBook.computeIfAbsent(book, k -> new ArrayList<>()).add(e);
        }

        for (final Map.Entry<String, List<Entry>> section : byBook.entrySet()) {
            sb.append("## ").append(section.getKey()).append("\n\n");
            final List<Entry> pages = section.getValue();
            pages.sort(landingFirst());
            for (final Entry e : pages) {
                sb.append("- [").append(e.title).append("](").append(linkPrefix).append(e.relativePath).append(')');
                if (e.description != null && !e.description.isEmpty()) {
                    sb.append(": ").append(e.description);
                }
                sb.append('\n');
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static Comparator<Entry> landingFirst() {
        return Comparator.comparing((Entry e) -> e.relativePath.endsWith("/index.md")
                || e.relativePath.equals("index.md") ? 0 : 1)
                .thenComparing(e -> e.relativePath);
    }

    private static String bookOf(final String relativePath) {
        final int slash = relativePath.lastIndexOf('/');
        return slash < 0 ? "." : relativePath.substring(0, slash);
    }

    // ---- filesystem driver -------------------------------------------------

    /**
     * Scans {@code markdownRoot} for {@code .md} pages, builds an entry per page (title from its
     * first heading, description from its first prose paragraph), writes {@code llms.txt} at the
     * root, and returns the number of pages indexed.
     */
    int generateInto(final Path markdownRoot) throws IOException {
        return generateInto(markdownRoot, markdownRoot.resolve("llms.txt"));
    }

    /**
     * Scans {@code markdownRoot} and writes the index to {@code outFile} (which may be outside the
     * markdown tree, e.g. the site root when publishing with a versioned {@code linkPrefix}).
     */
    int generateInto(final Path markdownRoot, final Path outFile) throws IOException {
        final List<Entry> entries = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(markdownRoot)) {
            final List<Path> mdFiles = walk
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".md"))
                    .sorted()
                    .collect(Collectors.toList());
            for (final Path md : mdFiles) {
                final String rel = markdownRoot.relativize(md).toString().replace('\\', '/');
                final String content = new String(Files.readAllBytes(md), StandardCharsets.UTF_8);
                entries.add(new Entry(rel, extractTitle(content, rel), extractDescription(content)));
            }
        }
        final String body = generate(entries);
        Files.write(outFile, body.getBytes(StandardCharsets.UTF_8));
        if (body.length() > SIZE_CAP) {
            LOG.warning("llms.txt is " + body.length() + " chars, over the " + SIZE_CAP
                    + "-char cap; consider a two-level (per-book) index");
        }
        LOG.info("Wrote " + outFile + " indexing " + entries.size() + " pages (" + body.length() + " chars)");
        return entries.size();
    }

    /** First heading text, or the file name (sans .md) if the page has no heading. */
    static String extractTitle(final String content, final String relativePath) {
        for (final String line : content.split("\n", -1)) {
            final Matcher m = HEADING.matcher(line);
            if (m.matches()) return m.group(1).trim();
        }
        final String name = relativePath.substring(relativePath.lastIndexOf('/') + 1);
        return name.endsWith(".md") ? name.substring(0, name.length() - 3) : name;
    }

    /**
     * The page description for the index. Prefers an author-curated {@code <!-- llms-summary: ... -->}
     * comment (emitted by {@link MarkdownConverter} from a section's {@code [llms-summary="..."]}
     * attribute) when the page has one; otherwise falls back to the first prose paragraph: the first
     * non-blank line after the first heading that is not itself structural (anchor, heading, pointer,
     * fence, image, table). Inline links/code/HTML are flattened and the result trimmed to one
     * sentence.
     */
    static String extractDescription(final String content) {
        final String curated = extractCuratedSummary(content);
        if (!curated.isEmpty()) return curated;

        final String[] lines = content.split("\n", -1);
        boolean seenHeading = false;
        boolean inFence = false;
        for (final String raw : lines) {
            final String line = raw.trim();
            if (!seenHeading) {
                if (HEADING.matcher(line).matches()) seenHeading = true;
                continue;
            }
            // Skip the entire fenced code block, not just its opening delimiter.
            if (line.startsWith("```")) {
                inFence = !inFence;
                continue;
            }
            if (inFence) continue;
            if (line.isEmpty()) continue;
            if (ANCHOR.matcher(line).matches() || HEADING.matcher(line).matches()
                    || LLMS_POINTER_LINE.matcher(line).matches()
                    || line.startsWith("![")
                    || line.startsWith("<!--")
                    || line.startsWith("|") || line.startsWith(">")) {
                continue;
            }
            return firstSentence(flattenInline(line));
        }
        return "";
    }

    /**
     * Returns the first author-curated {@code <!-- llms-summary: ... -->} comment on the page (that
     * of the page's leading section), or an empty string if none. Curated text is used verbatim
     * (only un-escaping the {@code --&gt;} we escaped when emitting), not truncated to one sentence.
     */
    static String extractCuratedSummary(final String content) {
        for (final String raw : content.split("\n", -1)) {
            final Matcher m = LLMS_SUMMARY_COMMENT.matcher(raw.trim());
            if (m.matches()) {
                return m.group(1).trim().replace("--&gt;", "-->");
            }
        }
        return "";
    }

    private static String flattenInline(final String s) {
        String t = INLINE_LINK.matcher(s).replaceAll("$1");
        t = INLINE_CODE.matcher(t).replaceAll("$1");
        t = HTML_TAG.matcher(t).replaceAll("");
        return t.replaceAll("\\s+", " ").trim();
    }

    private static String firstSentence(final String s) {
        // Cut at the first sentence end followed by a space, capping length so the index stays small.
        final int cap = 200;
        final Matcher m = Pattern.compile("[.!?](\\s|$)").matcher(s);
        if (m.find() && m.start() + 1 <= cap) {
            return s.substring(0, m.start() + 1);
        }
        return s.length() <= cap ? s : s.substring(0, cap).trim() + "…";
    }

    /**
     * CLI entry point:
     * {@code LlmsTxtGenerator [--prefix P] [--out FILE] <markdown-root> [projectName] [summary]}.
     * Scans the root for {@code .md} pages and writes the index to {@code --out} (default
     * {@code <markdown-root>/llms.txt}); {@code --prefix} is prepended to every page link (e.g.
     * {@code docs/3.7.7/} for a site-root index).
     */
    public static void main(final String[] args) throws IOException {
        String prefix = "";
        String out = null;
        final List<String> pos = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if ("--prefix".equals(args[i]) && i + 1 < args.length) {
                prefix = args[++i];
            } else if ("--out".equals(args[i]) && i + 1 < args.length) {
                out = args[++i];
            } else {
                pos.add(args[i]);
            }
        }
        if (pos.isEmpty()) {
            System.err.println("usage: LlmsTxtGenerator [--prefix P] [--out FILE] <markdown-root> [projectName] [summary]");
            System.exit(2);
        }
        final Path root = Path.of(pos.get(0));
        final String project = pos.size() > 1 ? pos.get(1) : "Apache TinkerPop Documentation";
        final String summary = pos.size() > 2 ? pos.get(2)
                : "Documentation for Apache TinkerPop, a graph computing framework and the Gremlin graph traversal language.";
        final Path outFile = out != null ? Path.of(out) : root.resolve("llms.txt");
        new LlmsTxtGenerator(project, summary, prefix).generateInto(root, outFile);
    }
}
