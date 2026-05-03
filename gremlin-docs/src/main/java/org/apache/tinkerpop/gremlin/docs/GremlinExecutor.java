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

import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.BindingsCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * Wraps a {@link GremlinGroovyScriptEngine} to execute Gremlin code blocks and capture console-style output.
 * Maintains state across evaluations within a single document, matching the behavior of the old AWK pipeline
 * which piped the entire document through one Gremlin Console session.
 */
public class GremlinExecutor implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(GremlinExecutor.class);

    /**
     * Maximum number of results to display per traversal, matching the Gremlin Console's
     * {@code :set max-iteration 100} default used by the old docs preprocessor.
     * Can be changed per-block via {@code :set max-iteration N}.
     */
    private int maxIteration = 100;

    private final GremlinGroovyScriptEngine engine;
    private boolean hadoopInitialized;

    public GremlinExecutor() {
        // Load all GremlinPlugin customizers (hadoop, spark, etc.) so their imports
        // and bindings (hdfs, fs, spark, SparkGraphComputer, etc.) are available
        final List<Customizer> customizers = new ArrayList<>();
        final List<BindingsCustomizer> bindingsCustomizers = new ArrayList<>();
        for (final GremlinPlugin plugin : ServiceLoader.load(GremlinPlugin.class)) {
            plugin.getCustomizers("gremlin-groovy").ifPresent(c -> {
                for (final Customizer customizer : c) {
                    if (customizer instanceof BindingsCustomizer) {
                        bindingsCustomizers.add((BindingsCustomizer) customizer);
                    } else {
                        customizers.add(customizer);
                    }
                }
            });
        }
        this.engine = new GremlinGroovyScriptEngine(customizers.toArray(new Customizer[0]));

        // Set Hadoop's default filesystem to an isolated temp directory so that
        // hdfs.ls(), hdfs.copyFromLocal() etc. operate in a clean sandbox instead
        // of the user's home directory.
        java.io.File hadoopTmp = null;
        try {
            hadoopTmp = java.nio.file.Files.createTempDirectory("tinkerpop-docs-hdfs").toFile();
            hadoopTmp.deleteOnExit();
        } catch (final Exception e) {
            log.debug("Could not set up isolated HDFS directory", e);
        }

        // BindingsCustomizer is not handled by the engine constructor — apply manually.
        for (final BindingsCustomizer bc : bindingsCustomizers) {
            final Bindings bindings = bc.getBindings();
            bindings.forEach((k, v) -> engine.put(k, v));
        }

        // Override hdfs/fs bindings with FileSystemStorage rooted at the temp directory.
        // FileSystemStorage.ls() with no args lists fs.getHomeDirectory(), so we need a
        // filesystem whose home directory is our temp dir.
        if (hadoopTmp != null) {
            try {
                final String tmpPath = hadoopTmp.getAbsolutePath();
                engine.put("__docsHdfsRoot", tmpPath);
                // Use a RawLocalFileSystem subclass that overrides getHomeDirectory.
                // We define it in Groovy so it's available to the script engine.
                engine.eval(
                    "class DocsLocalFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {\n" +
                    "  private org.apache.hadoop.fs.Path home\n" +
                    "  DocsLocalFileSystem(String homeDir) {\n" +
                    "    super()\n" +
                    "    this.home = new org.apache.hadoop.fs.Path(homeDir)\n" +
                    "    initialize(java.net.URI.create('file:///'), new org.apache.hadoop.conf.Configuration())\n" +
                    "    setWorkingDirectory(home)\n" +
                    "  }\n" +
                    "  org.apache.hadoop.fs.Path getHomeDirectory() { home }\n" +
                    "}\n" +
                    "hdfs = org.apache.tinkerpop.gremlin.hadoop.structure.io.FileSystemStorage.open(new DocsLocalFileSystem(__docsHdfsRoot))\n" +
                    "fs = hdfs\n");
            } catch (final Exception e) {
                log.debug("Could not override hdfs binding", e);
            }
        }

        this.hadoopInitialized = false;

        // Load console utility functions (describeGraph, etc.) from gremlin-console
        try (final java.io.InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("org/apache/tinkerpop/gremlin/console/jsr223/UtilitiesGremlinPluginScript.groovy")) {
            if (is != null) {
                engine.eval(new String(is.readAllBytes()));
            }
        } catch (final Exception e) {
            log.debug("Could not load console utility functions", e);
        }
    }

    /**
     * Initializes the graph environment for a code block. The graph parameter corresponds to the second
     * attribute in {@code [gremlin-groovy,modern]} — e.g. "modern", "classic", "crew", "sink", "grateful",
     * or empty for a bare TinkerGraph. "existing" means reuse the current graph state.
     * <p>
     * Replicates the initialization from the old {@code init-code-blocks.awk}:
     * <ul>
     *   <li>Creates the graph via {@code TinkerFactory} or opens an empty {@code TinkerGraph}</li>
     *   <li>Creates a traversal source {@code g}</li>
     *   <li>Pre-binds {@code marko} vertex (if present in the graph) for convenience</li>
     *   <li>Cleans up {@code /tmp/tinkergraph.kryo} temp files</li>
     * </ul>
     */
    public void initGraph(final String graph) throws ScriptException {
        initGraph(graph, false);
    }

    /**
     * Initializes the graph environment with optional Hadoop/Spark support.
     *
     * @param graph  the graph name (modern, classic, etc.) or null for empty TinkerGraph
     * @param hadoop if true, configures a HadoopGraph with Spark in local mode
     */
    public void initGraph(final String graph, final boolean hadoop) throws ScriptException {
        if ("existing".equals(graph)) return;

        if (hadoop) {
            initHadoopGraph(graph);
            return;
        }

        // close previous graph if one exists
        try { engine.eval("if (graph != null && graph instanceof AutoCloseable) graph.close()"); }
        catch (final Exception ignored) { }

        if (graph != null && !graph.isEmpty()) {
            engine.eval("graph = org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.create" +
                    capitalize(graph) + "()");
        } else {
            engine.eval("graph = org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph.open()");
        }
        engine.eval("g = graph.traversal()");

        // pre-bind convenience variables matching init-code-blocks.awk
        engine.eval("marko = g.V().has('name', 'marko').tryNext().orElse(null)");
        engine.eval("f = new File('/tmp/tinkergraph.kryo'); if (f.exists()) f.deleteDir()");

    }

    /**
     * Initializes a HadoopGraph with Spark running in local mode. This enables execution of
     * OLAP examples that use {@code SparkGraphComputer} without requiring external Hadoop/Spark
     * infrastructure. The graph data is written to a temp file in Gryo format and read by
     * HadoopGraph via the local filesystem.
     */
    private void initHadoopGraph(final String graph) throws ScriptException {
        // close previous graph if one exists
        try { engine.eval("if (graph != null && graph instanceof AutoCloseable) graph.close()"); }
        catch (final Exception ignored) { }

        if (!hadoopInitialized) {
            // one-time setup: import hadoop/spark classes
            engine.eval("import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph\n" +
                    "import org.apache.tinkerpop.gremlin.hadoop.Constants\n" +
                    "import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat\n" +
                    "import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat\n" +
                    "import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer\n" +
                    "import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoRegistrator\n" +
                    "import org.apache.tinkerpop.gremlin.hadoop.structure.io.FileSystemStorage\n" +
                    "import org.apache.commons.configuration2.BaseConfiguration\n" +
                    "import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo\n");
            hadoopInitialized = true;
        }

        // write the TinkerFactory graph to a temp gryo file for HadoopGraph to read
        final String factoryMethod = (graph != null && !graph.isEmpty())
                ? "TinkerFactory.create" + capitalize(graph) + "()"
                : "TinkerGraph.open()";

        engine.eval(
                "tmpGraph = " + factoryMethod + "\n" +
                "tmpFile = File.createTempFile('tinkerpop-docs-', '.kryo')\n" +
                "tmpFile.deleteOnExit()\n" +
                "tmpGraph.io(GryoIo.build()).writeGraph(tmpFile.absolutePath)\n" +
                "tmpGraph.close()\n" +
                "\n" +
                "hadoopConf = new BaseConfiguration()\n" +
                "hadoopConf.setProperty('gremlin.graph', 'org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph')\n" +
                "hadoopConf.setProperty('gremlin.hadoop.graphReader', 'org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat')\n" +
                "hadoopConf.setProperty('gremlin.hadoop.graphWriter', 'org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat')\n" +
                "hadoopConf.setProperty('gremlin.hadoop.inputLocation', tmpFile.absolutePath)\n" +
                "hadoopConf.setProperty('gremlin.hadoop.outputLocation', 'output-' + System.currentTimeMillis())\n" +
                "hadoopConf.setProperty('gremlin.hadoop.jarsInDistributedCache', false)\n" +
                "hadoopConf.setProperty('gremlin.hadoop.defaultGraphComputer', 'org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer')\n" +
                "hadoopConf.setProperty('spark.master', 'local[4]')\n" +
                "hadoopConf.setProperty('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')\n" +
                "hadoopConf.setProperty('spark.kryo.registrator', 'org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoRegistrator')\n" +
                "\n" +
                "graph = HadoopGraph.open(hadoopConf)\n" +
                "g = traversal().with(graph).withComputer(SparkGraphComputer)\n");

    }

    /**
     * Executes a block of Gremlin code lines and returns console-style formatted output.
     * Multi-line statements (lines ending with {@code .} for method chaining) are joined before
     * evaluation. Results are formatted as {@code gremlin> line} followed by {@code ==>result}
     * lines, matching the Gremlin Console output format.
     * <p>
     * When a block contains {@code import} statements, the entire block is evaluated as a single
     * script since imports don't persist across separate {@code eval()} calls.
     */
    public String execute(final List<String> lines) throws ScriptException {
        // reset per-block settings
        maxIteration = 100;
        // check if block contains import statements — if so, evaluate as a single script
        final boolean hasImports = lines.stream().anyMatch(l -> l.trim().startsWith("import "));
        if (hasImports) {
            return executeAsScript(lines);
        }
        return executeLineByLine(lines);
    }

    /**
     * Evaluates the entire block as a single Groovy script. Used for blocks containing
     * {@code import} statements or complex Groovy constructs that don't work with
     * line-by-line evaluation.
     */
    private String executeAsScript(final List<String> lines) throws ScriptException {
        final StringBuilder output = new StringBuilder();
        final StringBuilder script = new StringBuilder();

        for (final String rawLine : lines) {
            final String trimmed = rawLine.replaceAll("(\\s*<\\d+>)+\\s*$", "").trim();
            if (trimmed.isEmpty()) continue;
            if (trimmed.startsWith(":")) continue;
            if (trimmed.startsWith("//")) continue;

            output.append("gremlin> ").append(trimmed).append("\n");
            script.append(trimmed).append("\n");
        }

        try {
            final Object result = engine.eval(script.toString());
            if (result != null) {
                formatResult(result, output);
            }
        } catch (final ScriptException e) {
            log.warn("Error evaluating gremlin script block", e);
            output.append("ERROR: ").append(e.getMessage()).append("\n");
        }

        return output.toString();
    }

    private String executeLineByLine(final List<String> lines) throws ScriptException {
        final StringBuilder output = new StringBuilder();
        final StringBuilder currentStatement = new StringBuilder();

        for (final String rawLine : lines) {
            // strip trailing AsciiDoc callout markers like <1>, <2>, or multiple <5> <6>
            final String trimmed = rawLine.replaceAll("(\\s*<\\d+>)+\\s*$", "").trim();
            if (trimmed.isEmpty()) continue;

            // handle :set max-iteration console command
            if (trimmed.startsWith(":set max-iteration")) {
                try {
                    maxIteration = Integer.parseInt(trimmed.split("\\s+")[2]);
                } catch (final Exception ignored) { }
                continue;
            }

            // skip other console commands like :plugin, etc.
            if (trimmed.startsWith(":")) continue;

            // skip comment lines
            if (trimmed.startsWith("//")) continue;

            // accumulate multi-line statements (lines ending with . are continuations)
            if (currentStatement.length() == 0) {
                output.append("gremlin> ").append(trimmed).append("\n");
            } else {
                output.append("   ").append(trimmed).append("\n");
            }
            currentStatement.append(trimmed).append("\n");

            // if line ends with a continuation character, keep accumulating
            if (isContinuationLine(trimmed, currentStatement.toString())) {
                continue;
            }

            // evaluate the complete statement
            final String stmt = currentStatement.toString();
            currentStatement.setLength(0);

            try {
                final Object result = engine.eval(stmt);
                if (result != null) {
                    formatResult(result, output);
                }
            } catch (final ScriptException e) {
                log.warn("Error evaluating gremlin: {}", stmt, e);
                output.append("ERROR: ").append(e.getMessage()).append("\n");
            }
        }

        // evaluate any remaining accumulated statement
        if (currentStatement.length() > 0) {
            try {
                final Object result = engine.eval(currentStatement.toString());
                if (result != null) {
                    formatResult(result, output);
                }
            } catch (final ScriptException e) {
                log.warn("Error evaluating gremlin: {}", currentStatement, e);
            }
        }

        return output.toString();
    }

    /**
     * Returns the raw Gremlin lines suitable for translation — strips comments, callout markers,
     * and multi-line continuations into single statements.
     */
    public static List<String> extractTranslatableLines(final List<String> lines) {
        final List<String> result = new ArrayList<>();
        final StringBuilder current = new StringBuilder();

        for (String line : lines) {
            // strip trailing callout markers like <1>, <2>, or multiple <5> <6>
            line = line.replaceAll("(\\s*<\\d+>)+\\s*$", "").trim();
            if (line.isEmpty() || line.startsWith("//") || line.startsWith(":")) continue;

            current.append(line).append("\n");

            if (!isContinuationLine(line, current.toString())) {
                result.add(current.toString().trim());
                current.setLength(0);
            }
        }

        if (current.length() > 0) {
            result.add(current.toString().trim());
        }

        return result;
    }

    /**
     * Determines if the current accumulated statement is incomplete and needs more lines.
     * Used by both {@link #executeLineByLine} and {@link #extractTranslatableLines}.
     * <p>
     * Note: counts brackets naively without respecting string literals.
     * Sufficient for typical Gremlin doc examples.
     */
    static boolean isContinuationLine(final String trimmedLine, final String accumulated) {
        if (trimmedLine.endsWith(".") || trimmedLine.endsWith("{") || trimmedLine.endsWith(",") ||
            trimmedLine.endsWith("(") || trimmedLine.endsWith("\\")) {
            return true;
        }
        return countChar(accumulated, '(') > countChar(accumulated, ')') ||
               countChar(accumulated, '[') > countChar(accumulated, ']') ||
               countChar(accumulated, '{') > countChar(accumulated, '}');
    }

    @Override
    public void close() {
        // GremlinGroovyScriptEngine does not implement Closeable/AutoCloseable.
        // Clean up graph and Hadoop/Spark resources if they were initialized.
        try { engine.eval("if (graph != null && graph instanceof AutoCloseable) graph.close()"); }
        catch (final Exception ignored) { }
        if (hadoopInitialized) {
            try {
                engine.eval("org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer.close()");
            } catch (final Exception ignored) { }
        }
    }

    private void formatResult(final Object result, final StringBuilder output) {
        if (result instanceof Iterator) {
            final Iterator<?> iter = (Iterator<?>) result;
            int count = 0;
            while (iter.hasNext() && count < maxIteration) {
                output.append("==>").append(iter.next()).append("\n");
                count++;
            }
        } else if (result instanceof Iterable) {
            int count = 0;
            for (final Object item : (Iterable<?>) result) {
                if (count >= maxIteration) break;
                output.append("==>").append(item).append("\n");
                count++;
            }
        } else if (result instanceof Stream) {
            ((Stream<?>) result).limit(maxIteration)
                    .forEach(item -> output.append("==>").append(item).append("\n"));
        } else {
            output.append("==>").append(result).append("\n");
        }
    }

    private static String capitalize(final String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    private static int countChar(final String s, final char c) {
        int count = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == c) count++;
        }
        return count;
    }
}
