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
package org.apache.tinkerpop.gremlin.docs;

import org.asciidoctor.ast.Block;
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.StructuralNode;
import org.asciidoctor.extension.Treeprocessor;
import org.apache.tinkerpop.gremlin.language.translator.Translator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * AsciidoctorJ {@link Treeprocessor} that processes {@code [gremlin-groovy,modern]} code blocks
 * in TinkerPop documentation. For each such block, it:
 * <ol>
 *   <li>Executes the Gremlin code in an embedded {@link GremlinExecutor} and captures console output</li>
 *   <li>Translates the canonical Gremlin to all language variants via {@link VariantTranslator}</li>
 *   <li>Wraps the console output and translations in a tabbed UI with proper AST listing blocks
 *       so Asciidoctor applies syntax highlighting via CodeRay</li>
 * </ol>
 */
public class GremlinTreeProcessor extends Treeprocessor {

    private static final Logger log = LoggerFactory.getLogger(GremlinTreeProcessor.class);
    private static final Pattern GREMLIN_STYLE = Pattern.compile("gremlin-(\\w+)");
    private static final AtomicLong counter = new AtomicLong(System.currentTimeMillis());

    @Override
    public Document process(final Document document) {
        final boolean dryRun = document.hasAttribute("gremlin-docs-dryrun");

        try (final GremlinExecutor executor = new GremlinExecutor()) {
            processNode(document, executor, dryRun);
        }

        return document;
    }

    private void processNode(final StructuralNode node, final GremlinExecutor executor, final boolean dryRun) {
        final List<StructuralNode> blocks = node.getBlocks();
        if (blocks == null || blocks.isEmpty()) return;

        for (int i = 0; i < blocks.size(); i++) {
            final StructuralNode child = blocks.get(i);

            if (child instanceof Block && isGremlinBlock((Block) child)) {
                i = processGremlinBlock(node, i, (Block) child, executor, dryRun);
            } else if (child instanceof Block && isTabStartBlock((Block) child)) {
                i = processStandaloneTabGroup(node, i);
            } else {
                processNode(child, executor, dryRun);
            }
        }
    }

    /**
     * Replaces a gremlin block with a sequence of AST nodes that form a tabbed view:
     * passthrough HTML for the tab structure interleaved with real listing blocks that
     * Asciidoctor will syntax-highlight.
     */
    private int processGremlinBlock(final StructuralNode parent, final int index,
                                     final Block block, final GremlinExecutor executor,
                                     final boolean dryRun) {
        final Matcher m = GREMLIN_STYLE.matcher(block.getStyle());
        if (!m.matches()) return index;

        final String lang = m.group(1);
        final String graph = getGraphAttribute(block);
        final boolean hadoop = isHadoopBlock(block);
        final List<String> lines = block.getLines();

        log.info("Processing [gremlin-{},{}{}] block ({} lines)", lang,
                graph != null ? graph : "", hadoop ? ",hadoop" : "", lines.size());

        // execute the gremlin code
        String consoleOutput;
        if (dryRun || isConsoleCommandBlock(lines)) {
            consoleOutput = formatDryRun(lines);
        } else {
            try {
                executor.initGraph(graph, hadoop);
                consoleOutput = executor.execute(lines);
            } catch (final Exception e) {
                log.error("Failed to execute gremlin block", e);
                consoleOutput = formatDryRun(lines);
            }
        }

        // collect tab entries: label + language + code content
        final List<TabEntry> tabs = new ArrayList<>();
        tabs.add(new TabEntry("console", "groovy", consoleOutput));

        // translate to language variants (available on 4.0+ with ANTLR-based translator)
        final List<String> translatableLines = GremlinExecutor.extractTranslatableLines(lines);
        if (!translatableLines.isEmpty()) {
            final Map<Translator, String> translations = VariantTranslator.translateBlock(translatableLines);
            for (final Map.Entry<Translator, String> entry : translations.entrySet()) {
                tabs.add(new TabEntry(
                        VariantTranslator.getDisplayName(entry.getKey()),
                        VariantTranslator.getSourceLanguage(entry.getKey()),
                        entry.getValue()));
            }
        }

        // consume any following [source,LANG,tab] blocks
        final List<StructuralNode> siblings = parent.getBlocks();
        int nextIndex = index + 1;
        while (nextIndex < siblings.size()) {
            final StructuralNode next = siblings.get(nextIndex);
            if (next instanceof Block && isManualTabBlock((Block) next)) {
                final Block tabBlock = (Block) next;
                final String tabLang = getSourceLanguage(tabBlock);
                tabs.add(new TabEntry(
                        tabLang != null ? tabLang : "code",
                        tabLang,
                        String.join("\n", tabBlock.getLines())));
                nextIndex++;
            } else {
                break;
            }
        }

        // build the replacement AST nodes
        final List<StructuralNode> replacements = buildTabbedBlocks(parent, tabs);

        // replace original block and consumed tab blocks with the new sequence
        // remove consumed blocks first (backwards to preserve indices)
        for (int j = nextIndex - 1; j > index; j--) {
            siblings.remove(j);
        }
        // remove the original gremlin block
        siblings.remove(index);
        // insert replacements at the same position
        siblings.addAll(index, replacements);

        // return last index of inserted blocks so the loop continues after them
        return index + replacements.size() - 1;
    }

    /**
     * Builds a sequence of AST blocks: passthrough HTML for tab structure interleaved
     * with real listing blocks for syntax-highlighted code.
     */
    private List<StructuralNode> buildTabbedBlocks(final StructuralNode parent, final List<TabEntry> tabs) {
        final List<StructuralNode> nodes = new ArrayList<>();

        final long id = counter.incrementAndGet();
        final int numTabs = tabs.size();

        // opening HTML: section + radio buttons + labels + first tab content div open
        final StringBuilder openHtml = new StringBuilder();
        openHtml.append("<section class=\"tabs tabs-").append(numTabs).append("\">\n");
        for (int i = 0; i < numTabs; i++) {
            final int tabNum = i + 1;
            final String checked = (i == 0) ? " checked=\"checked\"" : "";
            openHtml.append("  <input id=\"tab-").append(id).append("-").append(tabNum)
                    .append("\" type=\"radio\" name=\"radio-set-").append(id)
                    .append("\" class=\"tab-selector-").append(tabNum).append("\"")
                    .append(checked).append(" />\n");
            openHtml.append("  <label for=\"tab-").append(id).append("-").append(tabNum)
                    .append("\" class=\"tab-label-").append(tabNum).append("\">")
                    .append(tabs.get(i).label).append("</label>\n");
        }
        openHtml.append("  <div class=\"tabcontent\">\n    <div class=\"tabcontent-1\">\n");
        nodes.add(createBlock(parent, "pass", openHtml.toString()));

        // first tab content (listing block)
        nodes.add(createListingBlock(parent, tabs.get(0).language, tabs.get(0).content));

        // remaining tabs: close previous div, open next div, listing block
        for (int i = 1; i < numTabs; i++) {
            final int tabNum = i + 1;
            final String divHtml = "    </div>\n  </div>\n" +
                    "  <div class=\"tabcontent\">\n    <div class=\"tabcontent-" + tabNum + "\">\n";
            nodes.add(createBlock(parent, "pass", divHtml));
            nodes.add(createListingBlock(parent, tabs.get(i).language, tabs.get(i).content));
        }

        // closing HTML
        nodes.add(createBlock(parent, "pass", "    </div>\n  </div>\n</section>"));

        return nodes;
    }

    /**
     * Creates a proper Asciidoctor source listing block by parsing AsciiDoc markup.
     * This ensures CodeRay syntax highlighting is applied, since the block goes through
     * Asciidoctor's normal parsing pipeline.
     */
    private Block createListingBlock(final StructuralNode parent, final String language, final String content) {
        final List<String> lines = new ArrayList<>();
        lines.add("[source," + language + "]");
        lines.add("----");
        for (final String line : content.split("\n", -1)) {
            lines.add(line);
        }
        lines.add("----");
        final int sizeBefore = parent.getBlocks().size();
        parseContent(parent, lines);
        final List<StructuralNode> blocks = parent.getBlocks();
        if (blocks.size() > sizeBefore) {
            return (Block) blocks.remove(blocks.size() - 1);
        }
        // fallback if parseContent produced nothing
        return (Block) createBlock(parent, "listing", content);
    }

    private boolean isGremlinBlock(final Block block) {
        final String style = block.getStyle();
        return style != null && GREMLIN_STYLE.matcher(style).matches();
    }

    /**
     * Checks if a block starts a standalone tab group: {@code [source,LANG,tab]}.
     */
    private boolean isTabStartBlock(final Block block) {
        if (!"source".equals(block.getStyle())) return false;
        final Map<String, Object> attrs = block.getAttributes();
        // "tab" can appear as attribute "2" or "3" depending on how asciidoctor parses positions
        return "tab".equals(attrs.get("2")) || "tab".equals(attrs.get("3"));
    }

    /**
     * Checks if a block is a continuation of a tab group: a {@code [source,LANG]} block
     * whose language hasn't already been seen in the group.
     */
    private boolean isTabContinuationBlock(final Block block, final java.util.Set<String> seenLanguages) {
        if (!"source".equals(block.getStyle())) return false;
        final String lang = getSourceLanguage(block);
        return lang != null && !seenLanguages.contains(lang);
    }

    private boolean isManualTabBlock(final Block block) {
        if (!"source".equals(block.getStyle())) return false;
        final Map<String, Object> attrs = block.getAttributes();
        return "tab".equals(attrs.get("2")) || "tab".equals(attrs.get("3"));
    }

    /**
     * Processes a standalone tab group starting with {@code [source,LANG,tab]} and collecting
     * all consecutive {@code [source,LANG]} blocks into a tabbed view.
     */
    private int processStandaloneTabGroup(final StructuralNode parent, final int index) {
        final List<StructuralNode> siblings = parent.getBlocks();
        final List<TabEntry> tabs = new ArrayList<>();

        // collect the first block and all consecutive source blocks
        final Set<String> seenLanguages = new HashSet<>();
        int nextIndex = index;
        while (nextIndex < siblings.size()) {
            final StructuralNode node = siblings.get(nextIndex);
            if (!(node instanceof Block)) break;
            final Block block = (Block) node;

            if (nextIndex == index) {
                // first block must be a tab-start block
                if (!isTabStartBlock(block)) break;
            } else {
                // subsequent blocks must be source blocks with a unique language
                if (!isTabContinuationBlock(block, seenLanguages)) break;
            }

            final String lang = getSourceLanguage(block);
            final String label = lang != null ? lang : "code";
            if (lang != null) seenLanguages.add(lang);
            tabs.add(new TabEntry(label, lang, String.join("\n", block.getLines())));
            nextIndex++;
        }

        if (tabs.size() <= 1) return index; // not enough blocks for tabs

        log.info("Processing standalone tab group ({} tabs)", tabs.size());

        final List<StructuralNode> replacements = buildTabbedBlocks(parent, tabs);

        // remove original blocks (backwards)
        for (int j = nextIndex - 1; j >= index; j--) {
            siblings.remove(j);
        }
        siblings.addAll(index, replacements);

        return index + replacements.size() - 1;
    }

    private String getGraphAttribute(final Block block) {
        final Map<String, Object> attrs = block.getAttributes();
        Object attr = attrs.get("2");
        if (attr == null) attr = attrs.get(2);
        if (attr == null || "false".equals(attr.toString()) || attr.toString().isEmpty()) return null;
        return attr.toString();
    }

    /**
     * Checks if a gremlin block has the "hadoop" attribute, e.g. {@code [gremlin-groovy,modern,hadoop]}.
     * The "hadoop" flag appears as the third positional attribute.
     */
    private boolean isHadoopBlock(final Block block) {
        final Map<String, Object> attrs = block.getAttributes();
        Object attr = attrs.get("3");
        if (attr == null) attr = attrs.get(3);
        return "hadoop".equals(attr != null ? attr.toString() : null);
    }

    private String getSourceLanguage(final Block block) {
        // For [source,LANG], asciidoctor may store the language in "language" attr,
        // attribute "1" (which may contain "source"), or attribute "2".
        final Object langAttr = block.getAttribute("language");
        if (langAttr != null) return langAttr.toString();
        final Map<String, Object> attrs = block.getAttributes();
        // attribute "1" is often the style name itself; "2" has the language
        final Object attr2 = attrs.get("2");
        if (attr2 != null && !"tab".equals(attr2.toString()) && !"false".equals(attr2.toString())) {
            return attr2.toString();
        }
        final Object attr1 = attrs.get("1");
        if (attr1 != null && !"source".equals(attr1.toString())) return attr1.toString();
        return null;
    }

    /**
     * Detects blocks that contain console commands ({@code :remote}, {@code :>},
     * {@code :submit}) which cannot be executed in an embedded engine. These are rendered
     * as static code blocks with {@code gremlin>} prompts.
     */
    private static boolean isConsoleCommandBlock(final List<String> lines) {
        for (final String line : lines) {
            final String trimmed = line.trim();
            if (trimmed.startsWith(":remote") || trimmed.startsWith(":>") || trimmed.startsWith(":submit")) {
                return true;
            }
        }
        return false;
    }

    private static String formatDryRun(final List<String> lines) {
        final StringBuilder sb = new StringBuilder();
        for (final String line : lines) {
            sb.append("gremlin> ").append(line).append("\n");
        }
        return sb.toString();
    }

    private static class TabEntry {
        final String label;
        final String language;
        final String content;

        TabEntry(final String label, final String language, final String content) {
            this.label = label;
            this.language = language;
            this.content = content;
        }
    }
}
