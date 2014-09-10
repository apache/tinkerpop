package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.process.util.MapHelper;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * Base test class for Gremlin Process tests.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinProcessTest extends AbstractGremlinTest {

    /**
     * Determines if a test case implementation of a process test uses graph computer.  This value should be
     * set in the constructor of the class that implements this.
     */
    protected boolean requiresGraphComputer;

    /**
     * Determines if a graph meets requirements for execution.  All gremlin process tests should check this
     * method as part of a call to {@code assumeTrue} to ensure that the test doesn't require the computer
     * feature or if it does require the computer feature then ensure that the graph being tested supports it.
     */
    protected boolean graphMeetsTestRequirements() {
        return !requiresGraphComputer || g.features().graph().supportsComputer();
    }

    @Before
    public void setupTest() {
        assumeTrue(graphMeetsTestRequirements());
    }

    public <T> void checkResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
        final List<T> results = traversal.toList();
        assertEquals("Checking result size", expectedResults.size(), results.size());
        for (T t : results) {
            assertTrue("Checking result existence", expectedResults.contains(t));
        }
        final Map<T, Long> expectedResultsCount = new HashMap<>();
        final Map<T, Long> resultsCount = new HashMap<>();
        assertEquals("Checking indexing is equivalent", expectedResultsCount.size(), resultsCount.size());
        expectedResults.forEach(t -> MapHelper.incr(expectedResultsCount, t, 1l));
        results.forEach(t -> MapHelper.incr(resultsCount, t, 1l));
        expectedResultsCount.forEach((k, v) -> assertEquals("Checking result group counts", v, resultsCount.get(k)));
        assertFalse(traversal.hasNext());
    }

    public <T> void checkResults(final Map<T, Long> expectedResults, final Traversal<?, T> traversal) {
        final List<T> list = new ArrayList<>();
        expectedResults.forEach((k, v) -> {
            for (int i = 0; i < v; i++) {
                list.add(k);
            }
        });
        checkResults(list, traversal);
    }
}
