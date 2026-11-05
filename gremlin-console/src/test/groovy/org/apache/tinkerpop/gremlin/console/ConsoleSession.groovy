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
package org.apache.tinkerpop.gremlin.console

import org.codehaus.groovy.tools.shell.IO
import org.jline.terminal.Size
import org.jline.terminal.impl.DumbTerminal

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.concurrent.TimeUnit

/**
 * A console running in this JVM on a dumb terminal. Lines are typed one at a time and each call waits for the next
 * prompt, so the output of a line can be checked on its own.
 */
class ConsoleSession {

    static final String PROMPT = "gremlin> "
    static final String STACK_TRACE_PROMPT = "Display stack trace? [yN]"
    static final long TIMEOUT_MILLIS = 30000

    final Output out = new Output()
    final Output err = new Output()
    final Output screen = new Output()

    private final PipedOutputStream keys = new PipedOutputStream()
    private final DumbTerminal terminal
    private final Thread thread
    private final PrintStream oldOut = System.out
    private final PrintStream oldErr = System.err
    private volatile Integer exitCode
    private volatile Throwable failure

    /**
     * Starts the console and waits for its first prompt.
     *
     * @param home the user home, which holds the history and groovysh's own state
     * @param scriptsAndArgs the scripts run before the prompt, as with {@code -i}
     */
    ConsoleSession(final Path home, final List<List<String>> scriptsAndArgs = null, final int columns = 120) {
        final PipedInputStream typed = new PipedInputStream(keys, 1 << 16)
        terminal = new DumbTerminal("gremlin-test", "dumb", typed, screen, StandardCharsets.UTF_8)
        terminal.size = new Size(columns, 40)

        final PrintStream outStream = new PrintStream(out, true, "UTF-8")
        final PrintStream errStream = new PrintStream(err, true, "UTF-8")
        System.setOut(outStream)
        System.setErr(errStream)
        final IO io = new IO(new ByteArrayInputStream(new byte[0]), outStream, errStream)
        io.verbosity = IO.Verbosity.QUIET

        thread = new Thread({
            try {
                exitCode = new Console(io, scriptsAndArgs, true).start(terminal, home.resolve(".gremlin_groovy_history"))
            } catch (Throwable t) {
                failure = t
            }
        }, "console-session")
        thread.daemon = true
        thread.start()
        screen.waitFor(PROMPT, 0)
    }

    /**
     * Types a line and waits for the given prompt, or for the stack trace question after an error. Returns the
     * lines the console printed to standard output for it.
     */
    List<String> send(final String line, final String prompt = PROMPT) {
        final int outMark = out.length()
        final int errMark = err.length()
        final int screenMark = screen.length()
        type(line)
        waitUntil("'$prompt' or the stack trace question after: $line") {
            screen.text().indexOf(prompt, screenMark) >= 0 || err.text().indexOf(STACK_TRACE_PROMPT, errMark) >= 0
        }
        lastErr = err.text().substring(errMark)
        // the terminal echoes the typed line first and ends with the next prompt
        final String onScreen = screen.text().substring(screenMark)
        lastShown = lines(onScreen.substring(Math.max(onScreen.indexOf("\n") + 1, 0)))
                .findAll { it != prompt.trim() && !it.startsWith(prompt) }
        return lines(out.text().substring(outMark))
    }

    String lastErr = ""

    /**
     * What the last {@link #send} wrote straight to the terminal, such as the output of the groovysh commands,
     * without the echo of the typed line and the next prompt.
     */
    List<String> lastShown = []

    /**
     * Like {@link #send} but only returns the result lines, i.e. those starting with {@code ==>}.
     */
    List<String> results(final String line, final String prompt = PROMPT) {
        return send(line, prompt).findAll { it.startsWith("==>") }
    }

    /**
     * Types a line that fails, checks the error output contains the message, then answers the stack trace question.
     * Returns the error output.
     */
    String fails(final String line, final String message) {
        final int errMark = err.length()
        send(line)
        err.waitFor(STACK_TRACE_PROMPT, errMark)
        final String error = err.text().substring(errMark)
        assert error.contains(message), "expected '$message' in: $error"
        send("n")
        return error
    }

    /**
     * Types a console command and returns what it printed, wherever it printed it, without the null result that every
     * command reports when it finishes.
     */
    List<String> command(final String line) {
        final List<String> printed = send(line).findAll { it != "==>null" }
        return printed + lines(lastErr) + lastShown
    }

    void type(final String line) {
        keys.write((line + "\n").getBytes(StandardCharsets.UTF_8))
        keys.flush()
    }

    int close() {
        if (exitCode == null) {
            keys.close()
            thread.join(TIMEOUT_MILLIS)
        }
        System.setOut(oldOut)
        System.setErr(oldErr)
        terminal.close()
        if (failure != null) throw new AssertionError("the console failed", failure)
        if (exitCode == null) throw new AssertionError("the console did not end; screen: " + screen.text())
        return exitCode
    }

    int waitForExit() {
        thread.join(TIMEOUT_MILLIS)
        return close()
    }

    private static final String ANSI_ESCAPE = /\u001b\[[0-9;?]*[A-Za-z]|\u001b[()][A-Za-z0-9]|\u001b[=>]/

    /**
     * The non-empty lines of the text, without color codes and other terminal escapes.
     */
    static List<String> lines(final String text) {
        return text.replaceAll(ANSI_ESCAPE, "").replace("\r\n", "\n").split("\n")
                .findAll { !it.trim().isEmpty() } as List<String>
    }

    static void waitUntil(final String what, final Closure<Boolean> condition) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MILLIS)
        while (!condition.call()) {
            if (System.nanoTime() > deadline) throw new AssertionError("timed out waiting for " + what)
            Thread.sleep(10)
        }
    }

    /**
     * Collects what is written to one stream so it can be searched while the console keeps writing.
     */
    static class Output extends OutputStream {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream()

        @Override
        synchronized void write(final int b) { bytes.write(b) }

        @Override
        synchronized void write(final byte[] b, final int off, final int len) { bytes.write(b, off, len) }

        synchronized String text() { bytes.toString("UTF-8") }

        int length() { text().length() }

        void waitFor(final String expected, final int from) {
            waitUntil("'$expected'") { text().indexOf(expected, from) >= 0 }
        }
    }
}
