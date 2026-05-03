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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Executes Gremlin code blocks by delegating to a long-running Gremlin Console process
 * ({@code bin/gremlin.sh}). Communicates via stdin/stdout using sentinel markers to
 * delimit individual statement boundaries. This provides the full console environment
 * including correct result formatting, Sugar plugin support, SPARQL, Neo4j, and remote
 * connections.
 * <p>
 * The console process is started once and reused across all code blocks in a document,
 * maintaining session state (variables, graph bindings) between blocks.
 * <p>
 * <b>Protocol:</b> Each statement is sent individually, followed by a sentinel marker.
 * The sentinel is sent twice to handle the console's "Display stack trace? [yN]" error
 * prompt, which reads the next stdin line as an answer. If a statement errors, the first
 * sentinel is consumed as the "N" answer and the second sentinel produces the expected
 * output. This per-statement approach prevents cascading failures where one error could
 * consume subsequent code lines or sentinels.
 */
public class ConsoleExecutor implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ConsoleExecutor.class);

    /**
     * Sentinel echoed after each statement to mark the end of output. Chosen to be
     * unlikely to appear in normal Gremlin output.
     */
    private static final String SENTINEL = "__GREMLIN_DOCS_BLOCK_END__";

    /** Pattern to strip ANSI escape codes from console output. */
    private static final Pattern ANSI_PATTERN = Pattern.compile("\u001B\\[[0-9;]*[a-zA-Z]");

    /** Pattern matching the gremlin prompt (with possible ANSI codes already stripped). */
    private static final Pattern PROMPT_PATTERN = Pattern.compile("^gremlin>\\s?");

    /** Pattern matching continuation prompts like {@code ......1> }. */
    private static final Pattern CONTINUATION_PATTERN = Pattern.compile("^\\.+\\d+>\\s?");

    private final Process process;
    private final BufferedWriter stdin;
    private final BufferedReader stdout;
    private final Thread stderrDrainer;

    /**
     * Creates a new ConsoleExecutor that launches {@code bin/gremlin.sh} from the given
     * console home directory.
     *
     * @param consoleHome path to the unpacked Gremlin Console distribution
     */
    public ConsoleExecutor(final String consoleHome) {
        this(consoleHome, null);
    }

    /**
     * Creates a new ConsoleExecutor with an optional {@code HADOOP_GREMLIN_LIBS} setting.
     *
     * @param consoleHome       path to the unpacked Gremlin Console distribution
     * @param hadoopGremlinLibs value for the HADOOP_GREMLIN_LIBS environment variable, or null
     */
    public ConsoleExecutor(final String consoleHome, final String hadoopGremlinLibs) {
        final Path consoleBin = Paths.get(consoleHome, "bin", "gremlin.sh");
        log.info("Starting Gremlin Console from {}", consoleBin);

        try {
            final ProcessBuilder pb = new ProcessBuilder(consoleBin.toString());
            pb.directory(Paths.get(consoleHome).toFile());
            pb.environment().put("TERM", "dumb");
            if (hadoopGremlinLibs != null) {
                pb.environment().put("HADOOP_GREMLIN_LIBS", hadoopGremlinLibs);
            }

            this.process = pb.start();
            this.stdin = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
            this.stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));
            final BufferedReader stderr = new BufferedReader(new InputStreamReader(process.getErrorStream()));

            // drain stderr in background to prevent blocking
            this.stderrDrainer = new Thread(() -> {
                try {
                    String line;
                    while ((line = stderr.readLine()) != null) {
                        log.debug("console stderr: {}", line);
                    }
                } catch (final Exception ignored) { }
            }, "console-stderr-drainer");
            stderrDrainer.setDaemon(true);
            stderrDrainer.start();

            // wait for the console to be ready by consuming the startup banner
            sendSentinel();
            consumeUntilSentinel();

            log.info("Gremlin Console started successfully");
        } catch (final Exception e) {
            throw new RuntimeException("Failed to start Gremlin Console from " + consoleHome, e);
        }
    }

    /**
     * Initializes the graph environment for a code block.
     *
     * @param graph the graph name (modern, classic, crew, sink, grateful) or null/empty for bare TinkerGraph.
     *              "existing" means reuse the current graph state.
     */
    public void initGraph(final String graph) {
        if ("existing".equals(graph)) return;

        executeQuietly("if (graph != null && graph instanceof AutoCloseable) graph.close()");

        if (graph != null && !graph.isEmpty()) {
            executeQuietly("graph = TinkerFactory.create" + capitalize(graph) + "()");
        } else {
            executeQuietly("graph = TinkerGraph.open()");
        }
        executeQuietly("g = graph.traversal()");
        executeQuietly("marko = g.V().has('name', 'marko').tryNext().orElse(null)");
        executeQuietly("f = new File('/tmp/tinkergraph.kryo'); if (f.exists()) f.deleteDir()");
        executeQuietly(":set max-iteration 100");
    }

    /**
     * Executes a block of Gremlin code lines and returns the console-formatted output.
     * Each statement is sent individually with its own sentinel boundary, so errors
     * on one statement cannot consume subsequent statements.
     * <p>
     * Multi-line statements (lines ending with {@code .}, open brackets, etc.) are
     * accumulated and sent as a single unit.
     */
    public String execute(final List<String> lines) {
        final StringBuilder output = new StringBuilder();
        final StringBuilder currentStatement = new StringBuilder();
        final List<String> promptLines = new ArrayList<>();

        for (final String rawLine : lines) {
            final String line = rawLine.replaceAll("(\\s*<\\d+>)+\\s*$", "").trim();
            if (line.isEmpty() || line.startsWith("//")) continue;

            // track prompt display for multi-line statements
            if (currentStatement.length() == 0) {
                promptLines.add("gremlin> " + line);
            } else {
                promptLines.add("   " + line);
            }
            currentStatement.append(line).append("\n");

            if (isContinuationLine(line, currentStatement.toString())) {
                continue;
            }

            // complete statement — send it and collect output
            final String stmtOutput = executeStatement(currentStatement.toString().trim());

            // build output: prompt lines followed by result lines
            for (final String pl : promptLines) {
                output.append(pl).append("\n");
            }
            if (!stmtOutput.isEmpty()) {
                output.append(stmtOutput);
            }

            currentStatement.setLength(0);
            promptLines.clear();
        }

        // flush any remaining accumulated statement
        if (currentStatement.length() > 0) {
            final String stmtOutput = executeStatement(currentStatement.toString().trim());
            for (final String pl : promptLines) {
                output.append(pl).append("\n");
            }
            if (!stmtOutput.isEmpty()) {
                output.append(stmtOutput);
            }
        }

        return output.toString();
    }

    /**
     * Returns the raw Gremlin lines suitable for translation — strips comments, callout markers,
     * and joins multi-line continuations into single statements.
     */
    public static List<String> extractTranslatableLines(final List<String> lines) {
        final List<String> result = new ArrayList<>();
        final StringBuilder current = new StringBuilder();

        for (String line : lines) {
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
        try {
            sendLine(":exit");
            stdin.flush();
            stdin.close();
        } catch (final Exception ignored) { }

        try {
            process.waitFor(10, TimeUnit.SECONDS);
        } catch (final InterruptedException ignored) { }

        if (process.isAlive()) {
            process.destroyForcibly();
        }
    }

    /**
     * Sends a single statement to the console, followed by a double sentinel, and reads
     * back only the result lines (everything between the prompt echo and the sentinel).
     * Returns the result lines (e.g. {@code ==>6\n}) or empty string if no results.
     */
    private String executeStatement(final String statement) {
        final StringBuilder result = new StringBuilder();
        try {
            sendLine(statement);
            sendSentinel();

            String line;
            while ((line = stdout.readLine()) != null) {
                line = stripAnsi(line);
                if (line.contains(SENTINEL)) break;

                // skip prompt lines — we build our own prompt display
                if (PROMPT_PATTERN.matcher(line).find()) continue;
                if (CONTINUATION_PATTERN.matcher(line).find()) continue;

                // capture result lines
                if (line.startsWith("==>")) {
                    result.append(line).append("\n");
                }
                // other non-prompt output (e.g. println from scripts) is included
                else if (!line.isEmpty()) {
                    result.append(line).append("\n");
                }
            }
        } catch (final Exception e) {
            log.error("Error executing statement: {}", statement, e);
        }
        return result.toString();
    }

    /**
     * Sends a statement and discards all output until the sentinel.
     */
    private void executeQuietly(final String statement) {
        try {
            sendLine(statement);
            sendSentinel();
            consumeUntilSentinel();
        } catch (final Exception e) {
            log.warn("Error during quiet execution: {}", statement, e);
        }
    }

    /**
     * Sends the sentinel marker twice. The double-send handles the case where the console
     * encounters an error and prompts "Display stack trace? [yN]" — that prompt reads the
     * next stdin line as the answer, consuming the first sentinel. The second sentinel
     * ensures we still see it in stdout and don't hang. The sentinel text doesn't start
     * with "y"/"Y", so no stack trace is printed.
     */
    private void sendSentinel() {
        sendLine("'" + SENTINEL + "'");
        sendLine("'" + SENTINEL + "'");
    }

    private void sendLine(final String line) {
        try {
            stdin.write(line);
            stdin.newLine();
            stdin.flush();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to write to console stdin", e);
        }
    }

    /**
     * Reads and discards stdout lines until the sentinel marker is found.
     */
    private void consumeUntilSentinel() {
        try {
            String line;
            while ((line = stdout.readLine()) != null) {
                line = stripAnsi(line);
                if (line.contains(SENTINEL)) return;
            }
        } catch (final Exception e) {
            log.warn("Error consuming console output", e);
        }
    }

    private static String stripAnsi(final String s) {
        return ANSI_PATTERN.matcher(s).replaceAll("");
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
