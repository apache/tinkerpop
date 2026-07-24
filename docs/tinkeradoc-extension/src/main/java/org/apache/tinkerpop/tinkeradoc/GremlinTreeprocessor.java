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
        m.put("theCrew", "graph = TinkerFactory.createTheCrew()");
        m.put("grateful", "graph = TinkerFactory.createGratefulDead()");
        m.put("sink", "graph = TinkerFactory.createKitchenSink()");
        GRAPH_INIT = Collections.unmodifiableMap(m);
    }

    private int gremlinBlockCount;
    private final StatementExecutor executor;
    private final TabbedHtmlBuilder tabBuilder;
    private final GremlinHighlighter highlighter;
    private final HtmlTabRenderer htmlTabRenderer;
    private final MarkdownTabRenderer markdownTabRenderer;
    private final GremlinExecutionCache executionCache;
    private final ConsoleRestartHandler restartHandler;
    private ConsoleRestartHandler activeRestartHandler;
    private String currentGraph;
    private String documentId;
    private List<String> currentExcludedPlugins;

    /**
     * Creates a GremlinTreeprocessor that processes blocks without executing them (dry-run mode).
     * Uses the shared process-wide execution cache (production/SPI path).
     */
    public GremlinTreeprocessor() {
        this((StatementExecutor) null, null, GremlinExecutionCache.SHARED);
    }

    /**
     * Creates a GremlinTreeprocessor that executes blocks against the provided console. Uses the
     * shared process-wide execution cache (production path) so live execution happens once across
     * the per-backend render passes.
     *
     * @param console the GremlinConsole to execute statements against, or null for dry-run
     */
    public GremlinTreeprocessor(final GremlinConsole console) {
        this(console == null ? null : statement -> console.execute(statement), null,
                GremlinExecutionCache.SHARED);
    }

    /**
     * Creates a GremlinTreeprocessor with a custom statement executor for testing. Uses a private
     * execution cache so cached tabs do not leak between tests.
     *
     * @param executor the executor to use, or null for dry-run
     */
    GremlinTreeprocessor(final StatementExecutor executor) {
        this(executor, null, new GremlinExecutionCache());
    }

    /**
     * Creates a GremlinTreeprocessor with a custom statement executor and restart handler for
     * testing. Uses a private execution cache so cached tabs do not leak between tests.
     *
     * @param executor       the executor to use, or null for dry-run
     * @param restartHandler the handler invoked when plugin exclusions change, or null to ignore
     */
    GremlinTreeprocessor(final StatementExecutor executor, final ConsoleRestartHandler restartHandler) {
        this(executor, restartHandler, new GremlinExecutionCache());
    }

    /**
     * Creates a GremlinTreeprocessor with an explicit execution cache. Tests use this to inject a
     * private cache so cached tabs do not leak between tests.
     *
     * @param executor       the executor to use, or null for dry-run
     * @param restartHandler the handler invoked when plugin exclusions change, or null to ignore
     * @param executionCache the cache of executed neutral tab groups, keyed per block
     */
    GremlinTreeprocessor(final StatementExecutor executor, final ConsoleRestartHandler restartHandler,
                         final GremlinExecutionCache executionCache) {
        this.executor = executor;
        this.tabBuilder = new TabbedHtmlBuilder();
        this.highlighter = new GremlinHighlighter();
        this.htmlTabRenderer = new HtmlTabRenderer(this.highlighter, this.tabBuilder);
        this.markdownTabRenderer = new MarkdownTabRenderer();
        this.executionCache = executionCache;
        this.restartHandler = restartHandler;
    }

    private StatementExecutor resolvedExecutor;
    private GremlinConsole lazyConsole;
    private Path consoleHomePath;
    private boolean dryRun;
    private boolean markdownMode;
    private boolean sugarLoaded;

    @Override
    public Document process(final Document document) {
        gremlinBlockCount = 0;
        currentGraph = null;
        documentId = resolveDocumentId(document);
        markdownMode = isMarkdownBackend(document);
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

        // Use an injected handler (tests) when present, otherwise default to physically toggling
        // plugin directories under the resolved console home (production via SPI).
        activeRestartHandler = restartHandler != null ? restartHandler
                : (consoleHomePath != null ? new PluginDirectoryRestartHandler(consoleHomePath) : null);

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
     * Resolves a stable identity for the source document, used to namespace execution-cache keys so
     * blocks from different books or files never collide. Prefers the {@code docfile} attribute (the
     * absolute source path, identical across backend passes); falls back to the document title, then
     * a constant, both of which still combine with the block ordinal and source hash in the key.
     */
    private static String resolveDocumentId(final Document document) {
        final Object docfile = document.getAttribute("docfile");
        if (docfile != null && !docfile.toString().isEmpty()) {
            return docfile.toString();
        }
        final String title = document.getTitle();
        return title != null && !title.isEmpty() ? title : "<no-docfile>";
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
            // Match the old preprocessor: raise the console's result display limit so traversals
            // returning many results (e.g. all-pairs shortestPath) render fully instead of being
            // truncated with "..." at the interactive default.
            lazyConsole.execute(":set max-iteration 100");
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
     * Establishes the document-level baseline exclusion set before the AST walk. An absent
     * attribute means "no exclusions", so each book starts with every plugin enabled and any
     * exclusion latched from a prior document is cleared.
     */
    private void checkPluginExclusions(final Document document) {
        if (activeRestartHandler == null) return;
        final Object attrValue = document.hasAttribute(PLUGINS_EXCLUDE_ATTR)
                ? document.getAttribute(PLUGINS_EXCLUDE_ATTR) : null;
        applyExclusion(parseExcludeList(attrValue == null ? "" : attrValue.toString()));
    }

    /**
     * Applies a section-level {@code :gremlin-docs-plugins-exclude:} attribute encountered during
     * the walk. Unlike the document baseline, an absent attribute on a section means "inherit"
     * (no change), so only sections that declare the attribute trigger a transition.
     */
    private void maybeApplySectionExclusion(final StructuralNode node) {
        if (activeRestartHandler == null || !"section".equals(node.getContext())) return;
        final Object exclude = node.getAttribute(PLUGINS_EXCLUDE_ATTR);
        if (exclude != null) {
            applyExclusion(parseExcludeList(exclude.toString()));
        }
    }

    /**
     * Transitions the active plugin exclusion set. When it changes, the current console is closed,
     * the restart handler toggles the plugin directories (and {@code plugins.txt}), and the next
     * gremlin block lazily starts a fresh console with the new classpath.
     */
    private void applyExclusion(final List<String> excludeList) {
        final List<String> current = currentExcludedPlugins == null
                ? Collections.emptyList() : currentExcludedPlugins;
        currentExcludedPlugins = excludeList;
        if (excludeList.equals(current)) return;
        closeConsole();
        invokeRestartHandler(excludeList);
        sugarLoaded = false;
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
            activeRestartHandler.onRestart(excludedPlugins);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to restart console with excluded plugins: " + excludedPlugins, e);
        }
    }

    private void processBlock(final StructuralNode node, final boolean dryRun) {
        maybeApplySectionExclusion(node);
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

        // Walk consecutive [source,<lang>] sibling blocks first (FR-5). This is pure AST work with
        // no execution: it fixes the block range to replace and captures each sibling's language
        // and source, which are needed whether the console output comes fresh or from the cache.
        int lastIndex = startIndex;
        final List<String[]> siblingSources = new ArrayList<>(); // [lang, source]
        for (int j = startIndex + 1; j < blocks.size(); j++) {
            final StructuralNode sibling = blocks.get(j);
            if (isManualTabBlock(sibling)) {
                final Block sourceBlock = (Block) sibling;
                siblingSources.add(new String[]{getSourceLanguage(sourceBlock), sourceBlock.getSource()});
                lastIndex = j;
            } else {
                break;
            }
        }

        // Execute once: on the first backend pass this runs Gremlin and caches the neutral tabs;
        // on later backend passes the cache hit skips the console entirely, so live execution
        // happens exactly once and both backends render from identical executed output.
        final String cacheKey = GremlinExecutionCache.key(documentId, gremlinBlockCount, gremlinBlock.getSource());
        final List<NeutralTab> tabs;
        if (executionCache.contains(cacheKey)) {
            tabs = executionCache.get(cacheKey);
        } else {
            final String consoleOutput = buildConsoleOutput(gremlinBlock, dryRun);
            tabs = new ArrayList<>();
            tabs.add(NeutralTab.console("groovy", consoleOutput));
            // Second tab with clean source code (no prompts/output)
            tabs.add(NeutralTab.source("groovy", gremlinBlock.getSource()));
            for (final String[] sibling : siblingSources) {
                tabs.add(NeutralTab.source(sibling[0], sibling[1]));
            }
            executionCache.put(cacheKey, tabs);
        }

        emitNeutralTabGroup(parent, startIndex, lastIndex, tabs);
        return startIndex;
    }

    /**
     * Processes consecutive standalone [source,<lang>,tab] blocks into a tab group (FR-7).
     * Returns the index to continue iteration from.
     */
    private int processStandaloneTabGroup(final StructuralNode parent, final int startIndex) {
        final List<StructuralNode> blocks = parent.getBlocks();
        final List<NeutralTab> tabs = new ArrayList<>();

        int lastIndex = startIndex;
        for (int j = startIndex; j < blocks.size(); j++) {
            final StructuralNode block = blocks.get(j);
            if (isStandaloneTabBlock(block) || isManualTabBlock(block)) {
                final Block sourceBlock = (Block) block;
                final String lang = getSourceLanguage(sourceBlock);
                tabs.add(NeutralTab.source(lang, sourceBlock.getSource()));
                lastIndex = j;
            } else {
                break;
            }
        }

        emitNeutralTabGroup(parent, startIndex, lastIndex, tabs);
        return startIndex;
    }

    /**
     * Renders a neutral tab group and replaces the source blocks with the result. The neutral tabs
     * are round-tripped through {@link NeutralTabCodec} (serialize then parse) before rendering, so
     * the JSON payload is the single source of truth for the rendered output. This exercises the
     * codec on every real block and is the seam where a future execution pass will persist the JSON
     * into a neutral custom block for a separate render pass to consume.
     */
    private void emitNeutralTabGroup(final StructuralNode parent, final int startIndex,
                                     final int endIndex, final List<NeutralTab> tabs) {
        final String json = NeutralTabCodec.serialize(tabs);
        final List<NeutralTab> resolvedTabs = NeutralTabCodec.parse(json);
        final String rendered = markdownMode
                ? markdownTabRenderer.render(resolvedTabs)
                : htmlTabRenderer.render(parent, resolvedTabs);
        replaceWithPassBlock(parent, startIndex, endIndex, rendered);
    }

    /**
     * Whether the active backend is the TinkerPop Markdown backend. Read from the {@code backend}
     * attribute, which is available at treeprocessor time (verified) and lets one execution feed
     * both the HTML and Markdown render passes.
     */
    private static boolean isMarkdownBackend(final Document document) {
        final Object backend = document.getAttribute("backend");
        return backend != null && backend.toString().startsWith("tpmarkdown");
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
     * Replaces blocks from startIndex to endIndex (inclusive) with a pass block containing the
     * pre-rendered content (raw HTML for the HTML backend, raw Markdown for the Markdown backend).
     * The block is created with a {@code :raw} content model so its body is emitted verbatim by the
     * active converter without further AsciiDoc substitution.
     */
    private void replaceWithPassBlock(final StructuralNode parent, final int startIndex, final int endIndex,
                                      final String content) {
        final List<StructuralNode> blocks = parent.getBlocks();
        for (int j = endIndex; j >= startIndex; j--) {
            blocks.remove(j);
        }
        final Map<String, Object> attrs = new HashMap<>();
        final Map<Object, Object> options = new HashMap<>();
        options.put("content_model", ":raw");
        final Block passBlock = createBlock(parent, "pass", content, attrs, options);
        blocks.add(startIndex, passBlock);
    }

    private String buildConsoleOutput(final Block block, final boolean dryRun) {
        try {
            return doBuildConsoleOutput(block, dryRun);
        } catch (final ConsoleRestartedException e) {
            // Console was restarted — retry the entire block once on the fresh console
            LOG.info("Retrying block after console restart");
            return doBuildConsoleOutput(block, dryRun);
        }
    }

    private String doBuildConsoleOutput(final Block block, final boolean dryRun) {
        if (!dryRun) {
            ensureConsoleStarted();
        }

        final String source = block.getSource();
        if (source == null || source.isEmpty()) {
            return "";
        }

        final String[] lines = source.split("\\r?\\n");
        final List<String> displayStatements = buildDisplayStatements(lines);
        final List<String> execStatements = buildStatements(lines);

        // Sugar syntax permanently mutates the Groovy metaclass and only takes effect when
        // SugarLoader.load() runs on a pristine metaclass -- i.e. BEFORE any traversal has been
        // used in the JVM. Since the console is long-lived and has executed prior blocks, always
        // restart to get a fresh console, then load sugar before initializing the graph/source.
        final boolean needsSugar = !dryRun && execStatements.stream().anyMatch(GremlinTreeprocessor::usesSugarSyntax);
        if (needsSugar && getActiveExecutor() != null) {
            restartConsole();
            executeSafely("org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader.load()");
            sugarLoaded = true;
            final String graphName = extractGraphName(block);
            initGraphIfNeeded(graphName);
        } else if (!dryRun && getActiveExecutor() != null) {
            if (sugarLoaded) {
                // Previous block loaded sugar; restart to get a clean metaclass.
                restartConsole();
                sugarLoaded = false;
            }
            final String graphName = extractGraphName(block);
            initGraphIfNeeded(graphName);
        }

        final StringBuilder output = new StringBuilder();
        for (int s = 0; s < displayStatements.size(); s++) {
            // Show original lines with callouts for display
            final String[] displayLines = displayStatements.get(s).split("\\r?\\n");
            for (int l = 0; l < displayLines.length; l++) {
                if (l == 0) {
                    output.append(PROMPT).append(displayLines[l]).append("\n");
                } else {
                    // Continuation lines: indent to align with first line (no prompt)
                    output.append("         ").append(displayLines[l]).append("\n");
                }
            }
            if (!dryRun && getActiveExecutor() != null) {
                final String result = executeSafely(execStatements.get(s));
                if (result != null && !result.isEmpty()) {
                    final String[] resultLines = result.split("\\r?\\n");
                    // Skip echo lines (first line + continuation prompts like ......N>)
                    for (int idx = 1; idx < resultLines.length; idx++) {
                        if (resultLines[idx].matches("^\\.{6}\\d+>.*")) continue;
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
        return groupStatements(lines, true);
    }

    private static int countOccurrences(final String str, final String sub) {
        int count = 0;
        int idx = 0;
        while ((idx = str.indexOf(sub, idx)) != -1) { count++; idx += sub.length(); }
        return count;
    }

    /**
     * Groups source lines into complete statements for display. Preserves callouts.
     */
    static List<String> buildDisplayStatements(final String[] lines) {
        return groupStatements(lines, false);
    }

    /**
     * Groups source lines into complete statements. A new statement starts only at a
     * non-indented line when the accumulated statement has balanced brackets and is not
     * inside a triple-quoted string. Tracking bracket depth keeps multi-line Groovy
     * constructs (e.g. {@code (1..10).each \{ ... \}}) together even though their closing
     * line is not indented, which would otherwise send an incomplete statement to the
     * console and hang at a continuation prompt.
     *
     * @param strip when {@code true}, callouts are stripped from the emitted text (for
     *              execution); when {@code false}, the original line is emitted (for display)
     */
    private static List<String> groupStatements(final String[] lines, final boolean strip) {
        final List<String> statements = new ArrayList<>();
        final StringBuilder current = new StringBuilder();
        boolean inTripleQuote = false;
        int depth = 0;
        for (final String line : lines) {
            final String detect = stripCallouts(line);
            final String content = strip ? detect : line;
            final int tqCount = countOccurrences(detect, "\"\"\"");
            final boolean touchesTriple = inTripleQuote || tqCount > 0;
            if (current.length() == 0) {
                current.append(content);
            } else if (inTripleQuote || depth > 0
                    || (detect.length() > 0 && Character.isWhitespace(detect.charAt(0)))) {
                current.append("\n").append(content);
            } else {
                statements.add(current.toString());
                current.setLength(0);
                depth = 0;
                current.append(content);
            }
            if (!touchesTriple) depth += bracketDelta(detect);
            if (tqCount % 2 != 0) inTripleQuote = !inTripleQuote;
        }
        if (current.length() > 0) {
            statements.add(current.toString());
        }
        return statements;
    }

    /**
     * Net change in bracket nesting ({@code (} {@code [} {@code \{}) contributed by a line,
     * ignoring brackets inside single- or double-quoted string literals (so GString
     * interpolation like {@code ${it}} does not affect the count).
     */
    private static int bracketDelta(final String s) {
        int d = 0;
        boolean sq = false;
        boolean dq = false;
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            if (sq) {
                if (c == '\\') i++;
                else if (c == '\'') sq = false;
            } else if (dq) {
                if (c == '\\') i++;
                else if (c == '"') dq = false;
            } else if (c == '\'') {
                sq = true;
            } else if (c == '"') {
                dq = true;
            } else if (c == '{' || c == '(' || c == '[') {
                d++;
            } else if (c == '}' || c == ')' || c == ']') {
                d--;
            }
        }
        return d;
    }

    /**
     * Strips AsciiDoc callout markers (e.g. {@code <1>}, {@code <2>}) from the end of a line.
     */
    static String stripCallouts(final String line) {
        return line.replaceAll("\\s*((<\\d+>\\s*)*<\\d+>)\\s*$", "");
    }

    /**
     * Detects sugar-syntax usage that requires SugarLoader: bare step properties
     * (e.g. {@code g.V}, {@code g.V.name}) or range operators (e.g. {@code g.V[0..2]}).
     */
    static boolean usesSugarSyntax(final String statement) {
        if (statement == null) return false;
        // Range access: g.V[...] or g.E[...]
        if (statement.matches(".*\\bg\\.[VE]\\s*\\[.*")) return true;
        // Bare step property: g.V or g.E not immediately followed by '('
        final java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\\bg\\.[VE]([^(\\w]|$)").matcher(statement);
        return m.find();
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
     * Initializes the graph and traversal source for the block. Re-initializes for
     * every non-{@code existing} block because prior blocks may have mutated
     * {@code graph} or reassigned {@code g} (e.g. {@code g = ...withComputer()}),
     * which would otherwise leak into subsequent blocks that expect a fresh source.
     */
    private void initGraphIfNeeded(final String graphName) {
        if (EXISTING.equals(graphName)) {
            return;
        }

        // Clear file-backed graph stores reused across blocks. Neo4j holds an exclusive
        // store lock, so a stale '/tmp/neo4j' left by a prior block (or prior build run)
        // makes the next Neo4jGraph.open('/tmp/neo4j') hang acquiring the lock. Close any
        // prior graph to release its lock, then delete the dirs -- mirroring the old
        // preprocessor which cleared these before every graph-init block.
        executeSafely("try { if (binding.hasVariable('graph') && graph != null) graph.close() } catch (e) {}");
        executeSafely("['/tmp/neo4j', '/tmp/tinkergraph.kryo'].each { p -> "
                + "def f = new File(p); if (f.exists()) f.deleteDir() }");

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
        } catch (final GremlinConsole.GremlinExecutionException e) {
            // A genuine Gremlin error must fail the build, not render as empty output.
            throw e;
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
        closeConsole();
        if (lazyConsoleWasClosed) {
            // Allow OS to reclaim resources from dead console and its children
            try { Thread.sleep(2000); } catch (final InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        currentGraph = null;
        ensureConsoleStarted();
    }

    private boolean lazyConsoleWasClosed;

    /** Closes the lazily-started console (if any) so the next block starts a fresh one. No-op in test mode. */
    private void closeConsole() {
        lazyConsoleWasClosed = lazyConsole != null;
        if (lazyConsole != null) {
            lazyConsole.close();
            lazyConsole = null;
            resolvedExecutor = null;
        }
        currentGraph = null;
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
