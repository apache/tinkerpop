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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.commons.lang3.StringUtils.repeat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TextTest {

    /**
     * The pattern {@code "a?"*N + "a"*N} over {@code "a"*N} needs about 2^N steps to match. At N=34 an uninterrupted
     * match runs for minutes, so a match that ends within the join window was stopped by the interrupt.
     */
    private static final int SLOW_MATCH_SIZE = 34;
    private static final String SLOW_PATTERN = repeat("a?", SLOW_MATCH_SIZE) + repeat('a', SLOW_MATCH_SIZE);
    private static final String SLOW_VALUE = repeat('a', SLOW_MATCH_SIZE);

    /**
     * Wrapping the match input in an interruptible {@code CharSequence} must not change what the predicate matches.
     */
    @Test
    public void shouldMatchRegexCorrectlyThroughInterruptibleWrapper() {
        final Text.RegexPredicate matches = new Text.RegexPredicate("^h.*o$", false);
        assertTrue(matches.test("hello", null));
        assertFalse(matches.test("world", null));

        // negate flips the result
        final Text.RegexPredicate notMatches = new Text.RegexPredicate("^h.*o$", true);
        assertFalse(notMatches.test("hello", null));
        assertTrue(notMatches.test("world", null));

        // a grouped pattern still evaluates to a simple find() boolean
        assertTrue(new Text.RegexPredicate("(\\d+)-(\\d+)", false).test("x 12-34 y", null));
        assertFalse(new Text.RegexPredicate("(\\d+)-(\\d+)", false).test("no digits here", null));
    }

    /**
     * Matching an input long enough for the interrupt flag to be checked during the match gives the expected results
     * when the thread is not interrupted.
     */
    @Test
    public void shouldMatchRegexCorrectlyOnInputLongerThanCheckInterval() {
        final String longInput = repeat('x', 5000) + "bob@example.com" + repeat('y', 5000);
        assertTrue(new Text.RegexPredicate("[a-z]+@example\\.com", false).test(longInput, null));
        assertFalse(new Text.RegexPredicate("@example\\.org", false).test(longInput, null));
        assertTrue(new Text.RegexPredicate("@example\\.org", true).test(longInput, null));
        assertFalse(new Text.RegexPredicate("[a-z]+@example\\.com", true).test(longInput, null));
    }

    /**
     * With the matching thread's interrupt flag set, matching must stop with a {@link TraversalInterruptedException}
     * and clear the flag. A plain scan over a long input is used so the throw can only come from the interrupt
     * check. Without that check the scan would finish promptly and the test would fail rather than hang.
     */
    @Test
    public void shouldAbortMatchWhenThreadAlreadyInterrupted() {
        // literal that never matches over a long run of 'a', so find() reads every position
        final Text.RegexPredicate predicate = new Text.RegexPredicate("z", false);
        final String value = repeat('a', 4096);

        Thread.currentThread().interrupt();
        try {
            predicate.test(value, null);
            fail("expected the interrupted scan to throw TraversalInterruptedException");
        } catch (TraversalInterruptedException expected) {
            assertFalse("interrupt flag should be cleared", Thread.currentThread().isInterrupted());
        } finally {
            // clear the flag so it does not leak into subsequent tests on this thread
            Thread.interrupted();
        }
    }

    /**
     * A long-running {@code regex} match on another thread stops when that thread is interrupted.
     */
    @Test
    public void shouldAbortLongRunningRegexWhenInterruptedMidMatch() throws Exception {
        assertMatchStopsWhenInterrupted(() -> {
            final Text.RegexPredicate predicate = new Text.RegexPredicate(SLOW_PATTERN, false);
            return () -> predicate.test(SLOW_VALUE, null);
        });
    }

    /**
     * A long-running {@code notRegex} match on another thread stops when that thread is interrupted.
     */
    @Test
    public void shouldAbortLongRunningNotRegexWhenInterruptedMidMatch() throws Exception {
        assertMatchStopsWhenInterrupted(() -> {
            final Text.RegexPredicate predicate = new Text.RegexPredicate(SLOW_PATTERN, true);
            return () -> predicate.test(SLOW_VALUE, null);
        });
    }

    /**
     * A traversal filtering with a long-running {@code TextP.regex()} match stops with a
     * {@link TraversalInterruptedException} when its thread is interrupted. Strategies are applied before the
     * interrupt so that it arrives while the match is running rather than before the step starts.
     */
    @Test
    public void shouldAbortTraversalWithLongRunningRegexWhenInterrupted() throws Exception {
        assertMatchStopsWhenInterrupted(() -> {
            final GraphTraversalSource g = EmptyGraph.instance().traversal();
            final GraphTraversal<String, String> traversal = g.inject(SLOW_VALUE).is(TextP.regex(SLOW_PATTERN));
            traversal.asAdmin().applyStrategies();
            return traversal::toList;
        });
    }

    /**
     * Runs a match on a separate daemon thread, interrupts it once the match is under way and asserts that the match
     * ends with a {@link TraversalInterruptedException}. The supplier runs on the match thread and does any setup,
     * returning the match itself, so that setup is complete before the interrupt is sent.
     */
    private static void assertMatchStopsWhenInterrupted(final Supplier<Runnable> prepareMatch) throws Exception {
        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        final CountDownLatch prepared = new CountDownLatch(1);
        final Thread matchThread = new Thread(() -> {
            try {
                final Runnable match = prepareMatch.get();
                prepared.countDown();
                match.run();
            } catch (Throwable t) {
                thrown.set(t);
            }
        }, "regex-match-test");
        // a match that ignores the interrupt must not keep the test JVM alive
        matchThread.setDaemon(true);

        matchThread.start();
        assertTrue("match thread did not finish its setup", prepared.await(5, TimeUnit.SECONDS));
        // give the match time to start before interrupting
        Thread.sleep(250);
        matchThread.interrupt();
        matchThread.join(TimeUnit.SECONDS.toMillis(15));

        assertFalse("regex match did not terminate within 15s of interrupt", matchThread.isAlive());
        assertTrue("expected TraversalInterruptedException but got: " + thrown.get(),
                thrown.get() instanceof TraversalInterruptedException);
    }
}
