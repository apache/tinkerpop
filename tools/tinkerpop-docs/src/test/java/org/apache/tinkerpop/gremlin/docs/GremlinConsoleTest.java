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

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link GremlinConsole} that verify prompt detection, timeout, and error dismissal
 * without requiring a real Gremlin Console process.
 */
public class GremlinConsoleTest {

    @Test
    public void shouldDetectPromptAndReturnOutput() throws Exception {
        final String fullStdout = "gremlin>" + "==>v[1]\n==>v[2]\ngremlin>";
        final GremlinConsole console = createConsole(fullStdout, "");

        try {
            final String result = console.execute("g.V()");
            assertThat(result, is("==>v[1]\n==>v[2]"));
        } finally {
            console.close();
        }
    }

    @Test
    public void shouldReturnEmptyForPromptOnly() throws Exception {
        final String fullStdout = "gremlin>" + "gremlin>";
        final GremlinConsole console = createConsole(fullStdout, "");

        try {
            final String result = console.execute("1+1");
            assertThat(result, is(""));
        } finally {
            console.close();
        }
    }

    @Test
    public void shouldHandleMultiLineOutput() throws Exception {
        final String fullStdout = "gremlin>" + "line1\nline2\nline3\ngremlin>";
        final GremlinConsole console = createConsole(fullStdout, "");

        try {
            final String result = console.execute("something");
            assertThat(result, is("line1\nline2\nline3"));
        } finally {
            console.close();
        }
    }

    @Test
    public void shouldTimeoutWhenNoPromptReceived() throws Exception {
        final PipedOutputStream feeder = new PipedOutputStream();
        final PipedInputStream stdoutStream = new PipedInputStream(feeder);

        // Write initial prompt so constructor succeeds
        feeder.write("gremlin>".getBytes(StandardCharsets.UTF_8));
        feeder.flush();

        final Process mockProcess = new MockProcess(
                stdoutStream,
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream());

        // Use 200ms timeout for fast test
        final GremlinConsole console = new GremlinConsole(mockProcess, 200);
        try {
            console.execute("g.V()");
            throw new AssertionError("Expected ConsoleTimeoutException");
        } catch (final GremlinConsole.ConsoleTimeoutException e) {
            assertThat(e.getMessage(), containsString("Timed out after 200ms"));
            assertThat(e.getMessage(), containsString("Buffer contents:"));
        } finally {
            console.close();
            feeder.close();
        }
    }

    @Test
    public void shouldIncludeBufferContentsInTimeoutMessage() throws Exception {
        final PipedOutputStream feeder = new PipedOutputStream();
        final PipedInputStream stdoutStream = new PipedInputStream(feeder);

        // Write initial prompt so constructor succeeds, then partial output
        feeder.write("gremlin>".getBytes(StandardCharsets.UTF_8));
        feeder.write("partial output here".getBytes(StandardCharsets.UTF_8));
        feeder.flush();

        final Process mockProcess = new MockProcess(
                stdoutStream,
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream());

        final GremlinConsole console = new GremlinConsole(mockProcess, 200);
        try {
            console.execute("test");
            throw new AssertionError("Expected ConsoleTimeoutException");
        } catch (final GremlinConsole.ConsoleTimeoutException e) {
            assertThat(e.getMessage(), containsString("partial output here"));
        } finally {
            console.close();
            feeder.close();
        }
    }

    @Test
    public void shouldDismissErrorPromptOnStderr() throws Exception {
        final String fullStdout = "gremlin>" + "gremlin>";
        final String stderrContent = "Display stack trace? [yN]";
        final ByteArrayOutputStream capturedStdin = new ByteArrayOutputStream();
        final GremlinConsole console = createConsoleWithStdin(fullStdout, stderrContent, capturedStdin);

        try {
            // Poll until the error dismisser has written to stdin, with timeout
            final long deadline = System.currentTimeMillis() + 5000;
            while (capturedStdin.size() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            final String result = console.execute("g.V()");
            assertThat(result, is(""));
            // Verify that a newline was sent to dismiss the error prompt
            final String sent = capturedStdin.toString(StandardCharsets.UTF_8.name());
            assertThat(sent.contains("\n"), is(true));
        } finally {
            console.close();
        }
    }

    @Test
    public void shouldThrowIOExceptionWhenProcessDiesMidRead() throws Exception {
        final PipedOutputStream feeder = new PipedOutputStream();
        final PipedInputStream stdoutStream = new PipedInputStream(feeder);

        // Write initial prompt so constructor succeeds
        feeder.write("gremlin>".getBytes(StandardCharsets.UTF_8));
        feeder.write("partial".getBytes(StandardCharsets.UTF_8));
        feeder.flush();
        // Close the stream to simulate process death (read returns -1)
        feeder.close();

        final Process mockProcess = new MockProcess(
                stdoutStream,
                new ByteArrayInputStream(new byte[0]),
                new ByteArrayOutputStream());

        final GremlinConsole console = new GremlinConsole(mockProcess, 5000);
        try {
            console.execute("g.V()");
            throw new AssertionError("Expected IOException");
        } catch (final IOException e) {
            assertThat(e.getMessage(), containsString("Console process ended unexpectedly"));
            assertThat(e.getMessage(), containsString("partial"));
        } finally {
            console.close();
        }
    }

    @Test
    public void shouldShutdownCleanly() throws Exception {
        final String fullStdout = "gremlin>";
        final GremlinConsole console = createConsole(fullStdout, "");
        console.close();
        // If we get here without hanging, shutdown is clean
    }

    private GremlinConsole createConsole(final String stdoutContent, final String stderrContent)
            throws IOException, GremlinConsole.ConsoleTimeoutException {
        return createConsoleWithStdin(stdoutContent, stderrContent, new ByteArrayOutputStream());
    }

    private GremlinConsole createConsoleWithStdin(final String stdoutContent, final String stderrContent,
                                                   final OutputStream stdinCapture)
            throws IOException, GremlinConsole.ConsoleTimeoutException {
        final InputStream stdoutStream = new ByteArrayInputStream(stdoutContent.getBytes(StandardCharsets.UTF_8));
        final InputStream stderrStream = new ByteArrayInputStream(stderrContent.getBytes(StandardCharsets.UTF_8));
        final Process mockProcess = new MockProcess(stdoutStream, stderrStream, stdinCapture);
        return new GremlinConsole(mockProcess, 5_000);
    }

    /**
     * A minimal Process mock that provides controlled streams.
     */
    private static class MockProcess extends Process {
        private final InputStream stdout;
        private final InputStream stderr;
        private final OutputStream stdin;

        MockProcess(final InputStream stdout, final InputStream stderr, final OutputStream stdin) {
            this.stdout = stdout;
            this.stderr = stderr;
            this.stdin = stdin;
        }

        @Override
        public OutputStream getOutputStream() {
            return stdin;
        }

        @Override
        public InputStream getInputStream() {
            return stdout;
        }

        @Override
        public InputStream getErrorStream() {
            return stderr;
        }

        @Override
        public int waitFor() {
            return 0;
        }

        @Override
        public boolean waitFor(final long timeout, final TimeUnit unit) {
            return true;
        }

        @Override
        public int exitValue() {
            return 0;
        }

        @Override
        public void destroy() {
        }

        @Override
        public Process destroyForcibly() {
            return this;
        }
    }
}
