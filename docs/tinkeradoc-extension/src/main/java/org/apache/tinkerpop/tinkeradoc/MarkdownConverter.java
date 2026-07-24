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

import org.asciidoctor.ast.Block;
import org.asciidoctor.ast.ContentNode;
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.List;
import org.asciidoctor.ast.ListItem;
import org.asciidoctor.ast.Section;
import org.asciidoctor.ast.StructuralNode;
import org.asciidoctor.ast.Table;
import org.asciidoctor.ast.Row;
import org.asciidoctor.ast.Cell;
import org.asciidoctor.converter.ConverterFor;
import org.asciidoctor.converter.StringConverter;

import java.util.Map;

/**
 * A GitHub-Flavored-Markdown backend for the TinkerPop docs. Registered for the {@code tpmarkdown}
 * backend, it walks the resolved AST and emits Markdown, so the same executed document renders to
 * both HTML (built-in backend) and Markdown from a single Gremlin execution (see
 * {@link GremlinExecutionCache}).
 * <p>
 * Gremlin tab groups arrive as {@code pass} blocks whose content the treeprocessor has already
 * rendered to Markdown (via {@link MarkdownTabRenderer}); those pass through verbatim. Prose,
 * headings, lists, admonitions, and simple tables are converted here. Complex tables that GFM pipe
 * syntax cannot represent fall back to a fenced text rendering.
 */
@ConverterFor("tpmarkdown")
public class MarkdownConverter extends StringConverter {

    /**
     * Section attribute holding an author-curated one-line summary for the llms.txt index, e.g.
     * {@code [llms-summary="..."]} on a section header. Emitted only as a hidden Markdown comment;
     * never rendered into the page body of either backend.
     */
    static final String LLMS_SUMMARY_ATTR = "llms-summary";

    public MarkdownConverter(final String backend, final Map<String, Object> opts) {
        super(backend, opts);
    }

    @Override
    public String convert(final ContentNode node, final String transform, final Map<Object, Object> opts) {
        if (node == null) return "";
        final String name = transform != null ? transform : node.getNodeName();

        // Debug hook: print every (transform,nodeName) the converter is asked to render.
        if (System.getProperty("tinkeradoc.md.debug") != null) {
            System.out.println("[MD-CONV] transform=" + transform + " nodeName=" + node.getNodeName());
        }

        switch (name) {
            case "document":
            case "embedded":
                return convertDocument((Document) node);
            case "section":
                return convertSection((Section) node);
            case "paragraph":
                return blockContent((StructuralNode) node) + "\n";
            case "ulist":
                return convertList((List) node, false);
            case "olist":
                return convertList((List) node, true);
            case "dlist":
                return convertDescriptionList((org.asciidoctor.ast.DescriptionList) node);
            case "colist":
                // Callout list: the tab renderers already inline callouts as // (n); a stray colist
                // elsewhere renders as a simple numbered list of its item texts.
                return convertList((List) node, true);
            case "listing":
            case "literal":
                return convertListing((Block) node);
            case "pass":
                // Treeprocessor-produced Markdown (e.g. a gremlin tab group) — emit verbatim.
                return ((Block) node).getContent() + "\n";
            case "admonition":
                return convertAdmonition((Block) node);
            case "table":
                return convertTable((Table) node);
            case "image":
                return convertImage((StructuralNode) node);
            case "inline_quoted":
                return convertInlineQuoted((org.asciidoctor.ast.PhraseNode) node);
            case "inline_anchor":
                return convertInlineAnchor((org.asciidoctor.ast.PhraseNode) node);
            case "inline_break":
                return ((org.asciidoctor.ast.PhraseNode) node).getText() + "\n";
            case "inline_callout":
                // Callouts inside gremlin tab groups are handled by the tab renderers; a stray
                // inline callout elsewhere renders as its number in parentheses.
                return "(" + ((org.asciidoctor.ast.PhraseNode) node).getText() + ")";
            case "inline_image":
                return convertInlineImage((org.asciidoctor.ast.PhraseNode) node);
            case "inline_footnote":
            case "inline_indexterm":
                return inlineText(node);
            default:
                return convertFallback(node);
        }
    }

    /** Renders an {@code inline_quoted} phrase (bold/italic/monospace/etc.) as Markdown. */
    private String convertInlineQuoted(final org.asciidoctor.ast.PhraseNode node) {
        final String text = node.getText() == null ? "" : node.getText();
        final String type = node.getType();
        if (type == null) return text;
        switch (type) {
            case "strong":       return "**" + text + "**";
            case "emphasis":     return "_" + text + "_";
            case "monospaced":   return "`" + text + "`";
            case "mark":         return text;
            case "superscript":  return "<sup>" + text + "</sup>";
            case "subscript":    return "<sub>" + text + "</sub>";
            default:              return text;
        }
    }

    /** Renders an {@code inline_anchor} (link/xref) as a Markdown link, or bare text if no target. */
    private String convertInlineAnchor(final org.asciidoctor.ast.PhraseNode node) {
        final String text = node.getText();
        final Object target = node.getTarget();
        if (target == null) return text == null ? "" : text;
        final String label = (text == null || text.isEmpty()) ? target.toString() : text;
        return "[" + label + "](" + target + ")";
    }

    private String inlineText(final ContentNode node) {
        if (node instanceof org.asciidoctor.ast.PhraseNode) {
            final String t = ((org.asciidoctor.ast.PhraseNode) node).getText();
            return t == null ? "" : t;
        }
        return "";
    }

    private String convertDocument(final Document doc) {
        final String content = doc.getContent() == null ? "" : doc.getContent().toString();
        final String title = doc.getDoctitle();
        final String assembled;
        // In a book, the document title is realized as a level-0 section inside the content, so it
        // already appears as an H1 there (matching the HTML backend's single <h1 class="sect0">).
        // In an article, the title is not part of the content, so emit it as the leading H1.
        if (title != null && !title.isEmpty() && !isBook(doc)) {
            final StringBuilder head = new StringBuilder();
            appendAnchor(head, doc.getId());
            head.append("# ").append(title).append("\n\n").append(content);
            assembled = head.toString();
        } else {
            assembled = content;
        }
        // Substitute the x.y.z version placeholder here rather than in the shared postprocessor:
        // AsciidoctorJ discards a postprocessor's return value for a custom StringConverter backend,
        // so the Markdown file would otherwise keep the literal x.y.z. The HTML backend still relies
        // on the postprocessor for the same substitution.
        return substituteVersion(doc, assembled);
    }

    /** Replaces {@code x.y.z} with the resolved TinkerPop version, if available. */
    private static String substituteVersion(final Document doc, final String text) {
        final Object version = firstNonNull(doc.getAttribute("tinkerpop-version"), doc.getAttribute("revnumber"));
        return version == null ? text : text.replace("x.y.z", version.toString());
    }

    private static Object firstNonNull(final Object a, final Object b) {
        return a != null ? a : b;
    }

    private static boolean isBook(final Document doc) {
        final Object doctype = doc.getAttribute("doctype");
        return "book".equals(String.valueOf(doctype));
    }

    private String convertSection(final Section section) {
        final StringBuilder sb = new StringBuilder();
        final int level = section.getLevel();
        final StringBuilder hashes = new StringBuilder();
        for (int i = 0; i <= level; i++) hashes.append('#');
        // Emit an explicit HTML anchor carrying the section's AsciiDoc id so xrefs of the form
        // [label](#the-id) resolve exactly (GFM renderers honor raw HTML anchors). This preserves
        // the ids verbatim, so no xref fragment remapping is needed, and it survives the WS2 page
        // split (each page keeps its headings' anchors).
        appendAnchor(sb, section.getId());
        sb.append(hashes).append(' ').append(section.getTitle()).append("\n\n");
        appendLlmsSummary(sb, section.getAttribute(LLMS_SUMMARY_ATTR));
        sb.append(section.getContent());
        return sb.toString();
    }

    /** Emits {@code <a id="..."></a>} on its own line when the node has a non-empty id. */
    private static void appendAnchor(final StringBuilder sb, final String id) {
        if (id != null && !id.isEmpty()) {
            sb.append("<a id=\"").append(id).append("\"></a>\n");
        }
    }

    /**
     * Emits a curated section summary as an HTML comment ({@code <!-- llms-summary: ... -->}) that
     * {@link LlmsTxtGenerator} reads for the page description. It is invisible in rendered Markdown
     * (and absent from HTML — the source attribute never reaches the HTML backend as content), so it
     * carries author-written index copy without polluting the page body. Authored in AsciiDoc as
     * {@code [llms-summary="..."]} on a section header.
     */
    private static void appendLlmsSummary(final StringBuilder sb, final Object summary) {
        if (summary == null) return;
        final String s = summary.toString().trim();
        if (s.isEmpty()) return;
        // Neutralize any comment-closer in author text so the comment can't be broken out of.
        sb.append("<!-- llms-summary: ").append(s.replace("-->", "--&gt;")).append(" -->\n\n");
    }

    private String convertList(final List list, final boolean ordered) {
        final StringBuilder sb = new StringBuilder();
        int index = 1;
        for (final StructuralNode item : list.getItems()) {
            final ListItem li = (ListItem) item;
            final String marker = ordered ? (index++ + ".") : "-";
            final String text = li.hasText() && li.getText() != null ? li.getText() : "";
            sb.append(marker).append(' ').append(text).append('\n');
            // Attached blocks (continuation paragraphs, nested lists) are converted individually and
            // indented. Converting each child block once — rather than calling li.getContent(), which
            // re-renders the item's whole subtree — avoids the quadratic/exponential blow-up that a
            // repeated getContent() causes on deep or dense structures.
            for (final StructuralNode child : li.getBlocks()) {
                sb.append(indent(child.convert()));
            }
        }
        sb.append('\n');
        return sb.toString();
    }

    /**
     * Renders a description (definition) list as {@code **term**} followed by an indented
     * description. Each term and description is converted once via its own node, never through a
     * blanket {@code getContent()} on the list (which re-renders and blows up on dense lists).
     */
    private String convertDescriptionList(final org.asciidoctor.ast.DescriptionList list) {
        final StringBuilder sb = new StringBuilder();
        for (final org.asciidoctor.ast.DescriptionListEntry entry : list.getItems()) {
            for (final ListItem term : entry.getTerms()) {
                final String t = term.hasText() && term.getText() != null ? term.getText() : "";
                sb.append("**").append(t).append("**\n");
            }
            final ListItem desc = entry.getDescription();
            if (desc != null) {
                if (desc.hasText() && desc.getText() != null) {
                    sb.append(indent(desc.getText())).append('\n');
                }
                for (final StructuralNode child : desc.getBlocks()) {
                    sb.append(indent(child.convert()));
                }
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private String convertListing(final Block block) {
        final String lang = languageOf(block);
        final StringBuilder sb = new StringBuilder();
        sb.append("```").append(lang).append('\n');
        sb.append(block.getSource());
        sb.append("\n```\n");
        return sb.toString();
    }

    private String convertAdmonition(final Block block) {
        final String style = block.getStyle();
        final String label = style != null ? style.toUpperCase() : "NOTE";
        final Object contentObj = block.getContent();
        final String body = contentObj == null ? "" : contentObj.toString();
        final StringBuilder sb = new StringBuilder();
        for (final String line : body.split("\n", -1)) {
            sb.append("> **").append(label).append(":** ").append(line).append('\n');
        }
        return sb.toString();
    }

    private String convertTable(final Table table) {
        // Only simple tables map cleanly to GFM pipe syntax. Fall back to a fenced text dump when
        // the table has multiple columns spanning or nested block content that pipes can't hold.
        try {
            final StringBuilder sb = new StringBuilder();
            final java.util.List<Row> header = table.getHeader();
            final java.util.List<Row> body = table.getBody();
            final int cols = columnCount(table);
            if (cols == 0) return "";

            if (!header.isEmpty()) {
                appendRow(sb, header.get(0), cols);
                appendSeparator(sb, cols);
            } else {
                // GFM requires a header row; synthesize a blank one so the table is valid.
                appendBlankHeader(sb, cols);
                appendSeparator(sb, cols);
            }
            for (final Row row : body) {
                appendRow(sb, row, cols);
            }
            sb.append('\n');
            return sb.toString();
        } catch (final RuntimeException e) {
            return convertTableFallback(table);
        }
    }

    private String convertTableFallback(final Table table) {
        final StringBuilder sb = new StringBuilder("```\n");
        for (final Row row : table.getBody()) {
            boolean first = true;
            for (final Cell cell : row.getCells()) {
                if (!first) sb.append(" | ");
                sb.append(cellText(cell));
                first = false;
            }
            sb.append('\n');
        }
        sb.append("```\n");
        return sb.toString();
    }

    private String convertImage(final StructuralNode node) {
        final Object alt = node.getAttribute("alt");
        return "![" + (alt == null ? "" : alt) + "](" + imagePath(node, node.getAttribute("target")) + ")\n";
    }

    /** Renders an inline image ({@code image:foo.png[alt]}) as a Markdown inline image. */
    private String convertInlineImage(final org.asciidoctor.ast.PhraseNode node) {
        final Object alt = node.getAttribute("alt");
        // Inline images carry their file in getTarget(), not the "target" attribute (unlike block
        // images), so prefer getTarget() and fall back to the attribute.
        Object target = node.getTarget();
        if (target == null) target = node.getAttribute("target");
        return "![" + (alt == null ? "" : alt) + "](" + imagePath(node, target) + ")";
    }

    /**
     * Resolves an image target against the document's {@code imagesdir}, matching the HTML backend
     * (which prepends {@code imagesdir} to every image src). Split pages of a book all live in the
     * book's directory, so the per-book {@code imagesdir} (e.g. {@code ../images}) is the correct
     * relative prefix for every page. Absolute or URL targets are left untouched.
     */
    private static String imagePath(final ContentNode node, final Object target) {
        if (target == null) return "";
        final String t = target.toString();
        if (t.isEmpty() || t.startsWith("/") || t.matches("^[a-zA-Z][a-zA-Z0-9+.-]*://.*")) {
            return t;
        }
        final Object dir = node.getDocument() == null ? null : node.getDocument().getAttribute("imagesdir");
        if (dir == null || dir.toString().isEmpty()) return t;
        final String d = dir.toString();
        return d.endsWith("/") ? d + t : d + "/" + t;
    }

    /**
     * Fallback for node types without a dedicated case. It converts the node's own text (if any) and
     * then each child block individually. It deliberately does <em>not</em> call {@code getContent()}
     * on the node: for container nodes {@code getContent()} re-renders the whole subtree, and when a
     * parent then also renders that content the work multiplies per nesting level, which produced an
     * out-of-memory blow-up on dense books. Converting each child exactly once is linear.
     */
    private String convertFallback(final ContentNode node) {
        if (!(node instanceof StructuralNode)) {
            return "";
        }
        final StructuralNode sn = (StructuralNode) node;
        final StringBuilder sb = new StringBuilder();
        if (sn instanceof Block) {
            final Object src = ((Block) sn).getSource();
            if (src != null && !src.toString().isEmpty()) {
                sb.append(src).append('\n');
            }
        }
        for (final StructuralNode child : sn.getBlocks()) {
            sb.append(child.convert());
        }
        return sb.toString();
    }

    private static String blockContent(final StructuralNode node) {
        final Object content = node.getContent();
        return content == null ? "" : content.toString();
    }

    private static String languageOf(final Block block) {
        final Object lang = block.getAttribute("language");
        return lang == null ? "" : lang.toString();
    }

    private int columnCount(final Table table) {
        if (!table.getHeader().isEmpty()) return table.getHeader().get(0).getCells().size();
        if (!table.getBody().isEmpty()) return table.getBody().get(0).getCells().size();
        return 0;
    }

    private void appendRow(final StringBuilder sb, final Row row, final int cols) {
        sb.append('|');
        final java.util.List<Cell> cells = row.getCells();
        for (int i = 0; i < cols; i++) {
            final String text = i < cells.size() ? cellText(cells.get(i)) : "";
            sb.append(' ').append(text.replace("\n", " ")).append(" |");
        }
        sb.append('\n');
    }

    private void appendBlankHeader(final StringBuilder sb, final int cols) {
        sb.append('|');
        for (int i = 0; i < cols; i++) sb.append("  |");
        sb.append('\n');
    }

    private void appendSeparator(final StringBuilder sb, final int cols) {
        sb.append('|');
        for (int i = 0; i < cols; i++) sb.append(" --- |");
        sb.append('\n');
    }

    private String cellText(final Cell cell) {
        final String t = cell.getText();
        return t == null ? "" : t;
    }

    private static String indent(final String content) {
        final StringBuilder sb = new StringBuilder();
        for (final String line : content.split("\n", -1)) {
            if (line.isEmpty()) {
                sb.append('\n');
            } else {
                sb.append("  ").append(line).append('\n');
            }
        }
        return sb.toString();
    }
}
