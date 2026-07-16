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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Manages a long-running Gremlin Console subprocess, sending statements and capturing output.
 * <p>
 * Uses prompt-based boundary detection: reads stdout until the {@code gremlin>} prompt appears.
 * Automatically dismisses {@code Display stack trace?} error prompts on stderr.
 */
public class GremlinConsole implements Closeable {

    static final String PROMPT = "gremlin>";
    static final String ERROR_PROMPT = "Display stack trace?";
    private static final long DEFAULT_TIMEOUT_MS = 90_000;
    private static final int EOF = -1;

    private final Process process;
    private final Writer stdin;
    private final BufferedReader stdout;
    private final BufferedReader stderr;
    private final Object stderrStdinLock = new Object();
    private final Thread errorDismisser;
    private final Thread stdoutReaderThread;
    private final BlockingQueue<Integer> stdoutQueue = new LinkedBlockingQueue<>();
    private final long timeoutMs;
    private volatile boolean running;
    private boolean inEscape;
    private volatile boolean errorPromptSeen;
    private volatile String lastErrorText = "";
    private final StringBuilder errorCapture = new StringBuilder();
    private static final int MAX_ERROR_CAPTURE = 8192;

    /**
     * Creates a new GremlinConsole by starting the {@code bin/gremlin.sh} subprocess.
     *
     * @param tinkerpopHome path to the TinkerPop distribution home directory
     */
    public GremlinConsole(final Path tinkerpopHome) throws IOException, ConsoleTimeoutException {
        this(new ProcessBuilder(tinkerpopHome.resolve("bin/gremlin.sh").toString())
                .redirectErrorStream(false)
                .start(), DEFAULT_TIMEOUT_MS);
    }

    /**
     * Package-private constructor for testing with a pre-built process.
     */
    GremlinConsole(final Process process, final long timeoutMs) throws IOException, ConsoleTimeoutException {
        this.process = process;
        this.stdin = new OutputStreamWriter(process.getOutputStream());
        this.stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));
        this.stderr = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        this.timeoutMs = timeoutMs;
        this.running = true;

        this.stdoutReaderThread = new Thread(this::readStdout, "gremlin-console-stdout-reader");
        this.stdoutReaderThread.setDaemon(true);
        this.stdoutReaderThread.start();

        this.errorDismisser = new Thread(this::dismissErrorPrompts, "gremlin-console-error-dismisser");
        this.errorDismisser.setDaemon(true);
        this.errorDismisser.start();

        // Wait for initial prompt
        readUntilPrompt();
    }

    /**
     * Sends a statement to the console and returns the output (excluding the prompt line).
     *
     * @param statement the Gremlin statement to execute
     * @return the console output for this statement
     * @throws IOException              if an I/O error occurs
     * @throws ConsoleTimeoutException  if the prompt is not received within the timeout period
     * @throws GremlinExecutionException if the statement produced a Gremlin error
     */
    public String execute(final String statement) throws IOException, ConsoleTimeoutException {
        synchronized (stderrStdinLock) {
            errorPromptSeen = false;
            lastErrorText = "";
            errorCapture.setLength(0);
        }
        stdin.write(statement + "\n");
        stdin.flush();
        final String output = readUntilPrompt();
        if (errorPromptSeen) {
            throw new GremlinExecutionException(statement, lastErrorText);
        }
        return output;
    }

    @Override
    public void close() {
        running = false;
        errorDismisser.interrupt();
        stdoutReaderThread.interrupt();
        process.destroyForcibly();
        try {
            process.waitFor(5, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        closeQuietly(stdin);
        closeQuietly(stdout);
        closeQuietly(stderr);
    }

    /**
     * Background thread that reads stdout and feeds characters into the blocking queue.
     */
    private void readStdout() {
        try {
            while (running) {
                final int ch = stdout.read();
                stdoutQueue.put(ch);
                if (ch == EOF) return;
            }
        } catch (final IOException | InterruptedException e) {
            // Process closed or interrupted, signal EOF
            try {
                stdoutQueue.put(EOF);
            } catch (final InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Reads from the stdout queue character-by-character until the prompt is detected.
     */
    private String readUntilPrompt() throws IOException, ConsoleTimeoutException {
        final StringBuilder buffer = new StringBuilder();
        final long deadline = System.currentTimeMillis() + timeoutMs;

        while (true) {
            final long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new ConsoleTimeoutException(
                        "Timed out after " + timeoutMs + "ms waiting for gremlin> prompt. Buffer contents:\n" + buffer);
            }

            final Integer ch;
            try {
                ch = stdoutQueue.poll(Math.min(remaining, 100), TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while reading console output", e);
            }

            if (ch == null) continue;

            if (ch == EOF) {
                throw new IOException("Console process ended unexpectedly. Buffer contents:\n" + buffer);
            }
            final char c = (char) ch.intValue();
            // Skip ANSI escape sequences (ESC [ ... letter)
            if (c == '\u001B') {
                inEscape = true;
                continue;
            }
            if (inEscape) {
                if (Character.isLetter(c)) {
                    inEscape = false;
                }
                continue;
            }
            buffer.append(c);

            if (buffer.length() >= PROMPT.length()) {
                final String tail = buffer.substring(buffer.length() - PROMPT.length());
                if (tail.equals(PROMPT)) {
                    final String output = buffer.substring(0, buffer.length() - PROMPT.length());
                    return output.trim();
                }
            }
        }
    }

    /**
     * Background thread that monitors stderr and automatically dismisses error prompts.
     * The {@code Display stack trace? [yN]} prompt is the signal that the current statement
     * raised a Gremlin error; the prompt is answered to unblock the process and the error is
     * recorded so {@link #execute(String)} can surface it as a {@link GremlinExecutionException}.
     * Error report text precedes the prompt on stderr, so it is captured for the message.
     */
    private void dismissErrorPrompts() {
        final int windowSize = ERROR_PROMPT.length();
        final StringBuilder errBuffer = new StringBuilder();
        try {
            while (running) {
                synchronized (stderrStdinLock) {
                    if (stderr.ready()) {
                        final int ch = stderr.read();
                        if (ch == -1) return;
                        errBuffer.append((char) ch);
                        if (errorCapture.length() < MAX_ERROR_CAPTURE) {
                            errorCapture.append((char) ch);
                        }

                        if (errBuffer.toString().contains(ERROR_PROMPT)) {
                            stdin.write("\n");
                            stdin.flush();
                            lastErrorText = errorCapture.toString().trim();
                            errorPromptSeen = true;
                            errBuffer.setLength(0);
                            errorCapture.setLength(0);
                        } else if (errBuffer.length() > windowSize) {
                            errBuffer.delete(0, errBuffer.length() - windowSize);
                        }
                    }
                }
                Thread.sleep(50);
            }
        } catch (final InterruptedException e) {
            // Expected on shutdown
        } catch (final IOException e) {
            // Process closed, exit silently
        }
    }

    private static void closeQuietly(final Closeable closeable) {
        try {
            if (closeable != null) closeable.close();
        } catch (final IOException ignored) {
            // ignore
        }
    }

    /**
     * Thrown when the console does not produce a prompt within the timeout period.
     */
    public static class ConsoleTimeoutException extends Exception {
        public ConsoleTimeoutException(final String message) {
            super(message);
        }
    }

    /**
     * @deprecated Use {@link ConsoleTimeoutException} instead.
     */
    @Deprecated
    public static class TimeoutException extends ConsoleTimeoutException {
        public TimeoutException(final String message) {
            super(message);
        }
    }

    /**
     * Thrown when an executed statement produced a Gremlin error (signalled by the console's
     * {@code Display stack trace? [yN]} prompt). Such errors must fail the docs build rather
     * than render as silently-empty output.
     */
    public static class GremlinExecutionException extends RuntimeException {
        public GremlinExecutionException(final String statement, final String errorText) {
            super("Gremlin statement failed: " + statement
                    + (errorText == null || errorText.isEmpty() ? "" : "\n" + errorText));
        }
    }
}
