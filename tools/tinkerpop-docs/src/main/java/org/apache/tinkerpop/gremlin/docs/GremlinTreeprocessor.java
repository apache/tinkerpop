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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Walks the AsciiDoc AST after parsing and finds listing blocks with style {@code gremlin-groovy}.
 * Executes the Gremlin code against a {@link GremlinConsole} and replaces block content with
 * formatted console output.
 */
public class GremlinTreeprocessor extends Treeprocessor {

    private static final Logger LOG = Logger.getLogger(GremlinTreeprocessor.class.getName());

    private static final String STYLE = "gremlin-groovy";
    private static final String PROMPT = "gremlin> ";
    private static final String EXISTING = "existing";
    private static final String TAB_ATTR = "tab";
    static final String PLUGINS_EXCLUDE_ATTR = "gremlin-docs-plugins-exclude";

    static final Set<String> SUPPORTED_LANGUAGES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("groovy", "java", "csharp", "javascript", "python", "go")));

    static final Map<String, String> GRAPH_INIT;

    static {
        final Map<String, String> m = new HashMap<>();
        m.put("modern", "graph = TinkerFactory.createModern()");
        m.put("classic", "graph = TinkerFactory.createClassic()");
        m.put("crew", "graph = TinkerFactory.createTheCrew()");
        m.put("grateful", "graph = TinkerFactory.createGratefulDead()");
        m.put("sink", "graph = TinkerFactory.createKitchenSink()");
        GRAPH_INIT = Collections.unmodifiableMap(m);
    }

    private int gremlinBlockCount;
    private final StatementExecutor executor;
    private final TabbedHtmlBuilder tabBuilder;
    private final ConsoleRestartHandler restartHandler;
    private String currentGraph;
    private List<String> currentExcludedPlugins;

    /**
     * Creates a GremlinTreeprocessor that processes blocks without executing them (dry-run mode).
     */
    public GremlinTreeprocessor() {
        this((StatementExecutor) null, null);
    }

    /**
     * Creates a GremlinTreeprocessor that executes blocks against the provided console.
     *
     * @param console the GremlinConsole to execute statements against, or null for dry-run
     */
    public GremlinTreeprocessor(final GremlinConsole console) {
        this(console == null ? null : statement -> console.execute(statement), null);
    }

    /**
     * Creates a GremlinTreeprocessor with a custom statement executor for testing.
     *
     * @param executor the executor to use, or null for dry-run
     */
    GremlinTreeprocessor(final StatementExecutor executor) {
        this(executor, null);
    }

    /**
     * Creates a GremlinTreeprocessor with a custom statement executor and restart handler.
     *
     * @param executor       the executor to use, or null for dry-run
     * @param restartHandler the handler invoked when plugin exclusions change, or null to ignore
     */
    GremlinTreeprocessor(final StatementExecutor executor, final ConsoleRestartHandler restartHandler) {
        this.executor = executor;
        this.tabBuilder = new TabbedHtmlBuilder();
        this.restartHandler = restartHandler;
    }

    private StatementExecutor resolvedExecutor;
    private GremlinConsole lazyConsole;
    private Path consoleHomePath;
    private boolean dryRun;

    @Override
    public Document process(final Document document) {
        gremlinBlockCount = 0;
        currentGraph = null;
        final Object dryRunAttr = document.getAttribute("gremlin-docs-dryrun");
        dryRun = dryRunAttr != null && !"false".equals(dryRunAttr.toString());

        // Store console home for lazy init on first gremlin block
        if (!dryRun && executor == null) {
            final Object consoleHome = document.getAttribute("gremlin-docs-console-home");
            if (consoleHome != null && !consoleHome.toString().isEmpty()) {
                consoleHomePath = Paths.get(consoleHome.toString());
            } else {
                LOG.info("No gremlin-docs-console-home attribute; skipping console execution");
            }
        }

        try {
            checkPluginExclusions(document);
            processBlock(document, dryRun);
            LOG.info("Processed " + gremlinBlockCount + " gremlin blocks");
        } finally {
            if (lazyConsole != null) {
                lazyConsole.close();
                lazyConsole = null;
                resolvedExecutor = null;
            }
        }
        return document;
    }

    /**
     * Starts the console on demand when the first gremlin block is encountered.
     */
    private void ensureConsoleStarted() {
        if (resolvedExecutor != null || executor != null) return;
        if (consoleHomePath == null) return;
        try {
            LOG.info("Starting GremlinConsole from: " + consoleHomePath);
            lazyConsole = new GremlinConsole(consoleHomePath);
            resolvedExecutor = statement -> lazyConsole.execute(statement);
            LOG.info("GremlinConsole started successfully");
        } catch (final IOException | GremlinConsole.ConsoleTimeoutException e) {
            LOG.warning("Failed to start GremlinConsole: " + e.getMessage());
            throw new ConsoleRestartedException("Console startup failed: " + e.getMessage());
        }
    }

    /**
     * Returns the number of gremlin-groovy listing blocks found during the last {@link #process} call.
     */
    public int getGremlinBlockCount() {
        return gremlinBlockCount;
    }

    /**
     * Checks the document for the {@code :gremlin-docs-plugins-exclude:} attribute and invokes
     * the restart handler if the exclusion list has changed.
     */
    private void checkPluginExclusions(final Document document) {
        if (restartHandler == null) return;
        if (!document.hasAttribute(PLUGINS_EXCLUDE_ATTR)) {
            if (currentExcludedPlugins != null) {
                currentExcludedPlugins = null;
                invokeRestartHandler(Collections.emptyList());
            }
            return;
        }

        final Object attrValue = document.getAttribute(PLUGINS_EXCLUDE_ATTR);
        final List<String> excludeList = parseExcludeList(attrValue == null ? "" : attrValue.toString());

        if (!excludeList.equals(currentExcludedPlugins)) {
            currentExcludedPlugins = excludeList;
            invokeRestartHandler(excludeList);
        }
    }

    /**
     * Parses a comma-separated list of plugin names into a sorted, deduplicated list.
     */
    static List<String> parseExcludeList(final String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .sorted()
                .distinct()
                .collect(Collectors.toList());
    }

    private void invokeRestartHandler(final List<String> excludedPlugins) {
        try {
            restartHandler.onRestart(excludedPlugins);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to restart console with excluded plugins: " + excludedPlugins, e);
        }
    }

    private void processBlock(final StructuralNode node, final boolean dryRun) {
        final List<StructuralNode> blocks = node.getBlocks();
        for (int i = 0; i < blocks.size(); i++) {
            final StructuralNode block = blocks.get(i);
            if ("listing".equals(block.getContext()) && STYLE.equals(block.getStyle())) {
                gremlinBlockCount++;
                LOG.info("Processing gremlin block #" + gremlinBlockCount);
                i = processGremlinTabGroup(node, i, dryRun);
            } else if (isStandaloneTabBlock(block)) {
                i = processStandaloneTabGroup(node, i);
            } else {
                processBlock(block, dryRun);
            }
        }
    }

    /**
     * Processes a gremlin-groovy block and any consecutive manual language variant siblings
     * into a tabbed HTML group. Returns the index to continue iteration from.
     */
    private int processGremlinTabGroup(final StructuralNode parent, final int startIndex, final boolean dryRun) {
        final List<StructuralNode> blocks = parent.getBlocks();
        final Block gremlinBlock = (Block) blocks.get(startIndex);

        final String consoleOutput = buildConsoleOutput(gremlinBlock, dryRun);
        final List<TabbedHtmlBuilder.Tab> tabs = new ArrayList<>();
        tabs.add(TabbedHtmlBuilder.consoleTabHighlighted("groovy",
                highlightAsGroovy(parent, consoleOutput)));
        // Add second tab with clean source code (no prompts/output)
        tabs.add(TabbedHtmlBuilder.codeTabHighlighted("groovy",
                highlightAsGroovy(parent, gremlinBlock.getSource())));

        // Consume consecutive [source,<lang>] sibling blocks as manual tabs (FR-5)
        int lastIndex = startIndex;
        for (int j = startIndex + 1; j < blocks.size(); j++) {
            final StructuralNode sibling = blocks.get(j);
            if (isManualTabBlock(sibling)) {
                final Block sourceBlock = (Block) sibling;
                final String lang = getSourceLanguage(sourceBlock);
                tabs.add(TabbedHtmlBuilder.codeTabHighlighted(lang,
                        highlightAsSource(parent, lang, sourceBlock.getSource())));
                lastIndex = j;
            } else {
                break;
            }
        }

        final String html = tabBuilder.build(tabs);
        replaceWithPassBlock(parent, startIndex, lastIndex, html);
        return startIndex;
    }

    /**
     * Highlights source code using CodeRay via the JRuby runtime bundled with AsciidoctorJ.
     */
    private String highlightAsGroovy(final StructuralNode parent, final String source) {
        return highlightAsSource(parent, "groovy", source);
    }

    private String highlightAsSource(final StructuralNode parent, final String lang, final String source) {
        if (source == null || source.isEmpty()) return "";
        // Strip callouts before highlighting, re-inject after
        final String[] lines = source.split("\\r?\\n");
        final String[] calloutMarkers = new String[lines.length];
        final StringBuilder cleanSource = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            final java.util.regex.Matcher m = java.util.regex.Pattern
                    .compile("\\s*((<\\d+>\\s*)+)$").matcher(lines[i]);
            if (m.find()) {
                calloutMarkers[i] = m.group(1);
                if (i > 0) cleanSource.append("\n");
                cleanSource.append(lines[i], 0, m.start());
            } else {
                calloutMarkers[i] = null;
                if (i > 0) cleanSource.append("\n");
                cleanSource.append(lines[i]);
            }
        }

        String highlighted = doHighlight(parent, lang, cleanSource.toString());

        // Re-inject callouts as HTML conums
        final String[] highlightedLines = highlighted.split("\\n", -1);
        final StringBuilder result = new StringBuilder();
        for (int i = 0; i < highlightedLines.length; i++) {
            if (i > 0) result.append("\n");
            result.append(highlightedLines[i]);
            if (i < calloutMarkers.length && calloutMarkers[i] != null) {
                final java.util.regex.Matcher nums = java.util.regex.Pattern
                        .compile("<(\\d+)>").matcher(calloutMarkers[i]);
                while (nums.find()) {
                    result.append(" <span class=\"hide-when-copy\">//</span> <b class=\"conum invisible\">(")
                          .append(nums.group(1)).append(")</b>");
                }
            }
        }
        return result.toString();
    }

    private String doHighlight(final StructuralNode parent, final String lang, final String source) {
        try {
            final org.jruby.Ruby ruby = org.asciidoctor.jruby.internal.JRubyRuntimeContext.get(parent);
            if (ruby == null) return escapeHtml(source);
            // Initialize CodeRay encoder once
            ruby.evalScriptlet("require 'coderay' unless defined?(CodeRay); " +
                    "$tp_coderay_groovy ||= CodeRay::Duo[:groovy, :html, :css => :class]");
            // Use heredoc to avoid escaping issues
            final String marker = "TPDOC" + System.identityHashCode(source);
            final String script = "$tp_coderay_groovy.highlight(<<'" + marker + "'.chomp\n" + source + "\n" + marker + "\n)";
            final org.jruby.runtime.builtin.IRubyObject result = ruby.evalScriptlet(script);
            return result != null ? result.asJavaString() : escapeHtml(source);
        } catch (final Exception e) {
            LOG.warning("CodeRay highlighting failed, falling back to plain: " + e.getMessage());
            return escapeHtml(source);
        }
    }

    private static String escapeHtml(final String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;").replace("\"", "&quot;");
    }

    /**
     * Processes consecutive standalone [source,<lang>,tab] blocks into a tab group (FR-7).
     * Returns the index to continue iteration from.
     */
    private int processStandaloneTabGroup(final StructuralNode parent, final int startIndex) {
        final List<StructuralNode> blocks = parent.getBlocks();
        final List<TabbedHtmlBuilder.Tab> tabs = new ArrayList<>();

        int lastIndex = startIndex;
        for (int j = startIndex; j < blocks.size(); j++) {
            final StructuralNode block = blocks.get(j);
            if (isStandaloneTabBlock(block)) {
                final Block sourceBlock = (Block) block;
                final String lang = getSourceLanguage(sourceBlock);
                tabs.add(TabbedHtmlBuilder.codeTab(lang, sourceBlock.getSource()));
                lastIndex = j;
            } else {
                break;
            }
        }

        final String html = tabBuilder.build(tabs);
        replaceWithPassBlock(parent, startIndex, lastIndex, html);
        return startIndex;
    }

    /**
     * Checks if a block is a [source,<lang>] listing block with a supported language (not tab-annotated).
     */
    private static boolean isManualTabBlock(final StructuralNode block) {
        if (!"listing".equals(block.getContext())) return false;
        if (!"source".equals(block.getStyle())) return false;
        final Object thirdAttr = block.getAttributes().get("3");
        if (thirdAttr != null && TAB_ATTR.equals(thirdAttr.toString().trim())) return false;
        final String lang = getSourceLanguage((Block) block);
        return lang != null && SUPPORTED_LANGUAGES.contains(lang);
    }

    /**
     * Checks if a block is a standalone tab block: [source,<lang>,tab].
     */
    private static boolean isStandaloneTabBlock(final StructuralNode block) {
        if (!"listing".equals(block.getContext())) return false;
        if (!"source".equals(block.getStyle())) return false;
        final Object thirdAttr = block.getAttributes().get("3");
        if (thirdAttr == null) return false;
        if (!TAB_ATTR.equals(thirdAttr.toString().trim())) return false;
        final String lang = getSourceLanguage((Block) block);
        return lang != null && SUPPORTED_LANGUAGES.contains(lang);
    }

    /**
     * Gets the source language from a listing block's attributes.
     */
    private static String getSourceLanguage(final Block block) {
        final Object langAttr = block.getAttributes().get("language");
        if (langAttr != null) {
            final String lang = langAttr.toString().trim();
            if (!lang.isEmpty()) return lang;
        }
        final Object attr = block.getAttributes().get("2");
        if (attr == null) return null;
        final String lang = attr.toString().trim();
        return lang.isEmpty() ? null : lang;
    }

    /**
     * Replaces blocks from startIndex to endIndex (inclusive) with a pass block containing raw HTML.
     */
    private void replaceWithPassBlock(final StructuralNode parent, final int startIndex, final int endIndex,
                                      final String html) {
        final List<StructuralNode> blocks = parent.getBlocks();
        for (int j = endIndex; j >= startIndex; j--) {
            blocks.remove(j);
        }
        final Map<String, Object> attrs = new HashMap<>();
        final Block passBlock = createBlock(parent, "pass", html, attrs);
        blocks.add(startIndex, passBlock);
    }

    private String buildConsoleOutput(final Block block, final boolean dryRun) {
        try {
            return doBuildConsoleOutput(block, dryRun);
        } catch (final ConsoleRestartedException e) {
            // Console was restarted — retry the entire block on the fresh console
            LOG.info("Retrying block after console restart");
            try {
                return doBuildConsoleOutput(block, dryRun);
            } catch (final ConsoleRestartedException e2) {
                // Block is genuinely broken — skip it
                LOG.warning("Block failed after retry, skipping: " + e2.getMessage());
                return buildDryRunOutput(block);
            }
        }
    }

    private String buildDryRunOutput(final Block block) {
        final String source = block.getSource();
        if (source == null || source.isEmpty()) return "";
        final StringBuilder output = new StringBuilder();
        for (final String line : source.split("\\r?\\n")) {
            output.append(PROMPT).append(line).append("\n");
        }
        return output.toString().stripTrailing();
    }

    private String doBuildConsoleOutput(final Block block, final boolean dryRun) {
        if (!dryRun) {
            ensureConsoleStarted();
        }
        if (!dryRun && getActiveExecutor() != null) {
            final String graphName = extractGraphName(block);
            initGraphIfNeeded(graphName);
        }

        final String source = block.getSource();
        if (source == null || source.isEmpty()) {
            return "";
        }

        final StringBuilder output = new StringBuilder();
        final String[] lines = source.split("\\r?\\n");
        final List<String> displayStatements = buildDisplayStatements(lines);
        final List<String> execStatements = buildStatements(lines);
        for (int s = 0; s < displayStatements.size(); s++) {
            // Show original lines with callouts for display
            for (final String displayLine : displayStatements.get(s).split("\\r?\\n")) {
                output.append(PROMPT).append(displayLine).append("\n");
            }
            if (!dryRun && getActiveExecutor() != null) {
                final String result = executeSafely(execStatements.get(s));
                if (result != null && !result.isEmpty()) {
                    final String[] resultLines = result.split("\\r?\\n");
                    // Skip the first line which is the echo of the command
                    for (int idx = 1; idx < resultLines.length; idx++) {
                        output.append(resultLines[idx]).append("\n");
                    }
                }
            }
        }
        return output.toString().stripTrailing();
    }

    /**
     * Groups source lines into complete statements for execution. Strips callouts.
     */
    static List<String> buildStatements(final String[] lines) {
        final List<String> statements = new ArrayList<>();
        final StringBuilder current = new StringBuilder();
        for (final String line : lines) {
            final String cleaned = stripCallouts(line);
            if (current.length() == 0) {
                current.append(cleaned);
            } else if (cleaned.length() > 0 && Character.isWhitespace(cleaned.charAt(0))) {
                current.append("\n").append(cleaned);
            } else {
                statements.add(current.toString());
                current.setLength(0);
                current.append(cleaned);
            }
        }
        if (current.length() > 0) {
            statements.add(current.toString());
        }
        return statements;
    }

    /**
     * Groups source lines into complete statements for display. Preserves callouts.
     */
    static List<String> buildDisplayStatements(final String[] lines) {
        final List<String> statements = new ArrayList<>();
        final StringBuilder current = new StringBuilder();
        for (final String line : lines) {
            final String stripped = stripCallouts(line);
            if (current.length() == 0) {
                current.append(line);
            } else if (stripped.length() > 0 && Character.isWhitespace(stripped.charAt(0))) {
                current.append("\n").append(line);
            } else {
                statements.add(current.toString());
                current.setLength(0);
                current.append(line);
            }
        }
        if (current.length() > 0) {
            statements.add(current.toString());
        }
        return statements;
    }

    /**
     * Strips AsciiDoc callout markers (e.g. {@code <1>}, {@code <2>}) from the end of a line.
     */
    static String stripCallouts(final String line) {
        return line.replaceAll("\\s*((<\\d+>\\s*)*<\\d+>)\\s*$", "");
    }

    /**
     * Extracts the graph name from the second positional attribute of the block.
     */
    static String extractGraphName(final StructuralNode block) {
        final Object attr = block.getAttributes().get("2");
        if (attr == null) {
            return null;
        }
        final String name = attr.toString().trim();
        return name.isEmpty() ? null : name;
    }

    /**
     * Initializes the graph in the console if the graph name has changed.
     */
    private void initGraphIfNeeded(final String graphName) {
        if (EXISTING.equals(graphName)) {
            return;
        }

        if (graphName != null && graphName.equals(currentGraph)) {
            return;
        }
        if (graphName == null && currentGraph == null && gremlinBlockCount > 1) {
            return;
        }

        final String initStatement;
        if (graphName == null) {
            initStatement = "graph = TinkerGraph.open()";
        } else {
            initStatement = GRAPH_INIT.getOrDefault(graphName, "graph = TinkerGraph.open()");
        }

        executeSafely(initStatement);
        executeSafely("g = graph.traversal()");
        currentGraph = graphName;
    }

    private StatementExecutor getActiveExecutor() {
        return executor != null ? executor : resolvedExecutor;
    }

    private String executeSafely(final String statement) {
        try {
            return getActiveExecutor().execute(statement);
        } catch (final GremlinConsole.ConsoleTimeoutException | IOException e) {
            // Console may have died — restart it and propagate to retry at block level
            LOG.warning("Console appears dead, restarting: " + e.getMessage());
            restartConsole();
            throw new ConsoleRestartedException("Console restarted due to: " + e.getMessage());
        } catch (final Exception e) {
            throw new RuntimeException("Failed to execute statement: " + statement, e);
        }
    }

    private void restartConsole() {
        if (lazyConsole != null) {
            lazyConsole.close();
            lazyConsole = null;
            resolvedExecutor = null;
        }
        currentGraph = null;
        ensureConsoleStarted();
    }

    /**
     * Thrown when the console was restarted mid-block to signal that the block should be retried.
     */
    static class ConsoleRestartedException extends RuntimeException {
        ConsoleRestartedException(final String message) {
            super(message);
        }
    }

    /**
     * Functional interface for executing Gremlin statements, enabling testability.
     */
    @FunctionalInterface
    interface StatementExecutor {
        String execute(String statement) throws IOException, GremlinConsole.ConsoleTimeoutException;
    }
}
