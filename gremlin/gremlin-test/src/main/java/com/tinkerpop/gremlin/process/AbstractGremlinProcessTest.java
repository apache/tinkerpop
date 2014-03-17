package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinTest;

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
        return !requiresGraphComputer || g.getFeatures().graph().supportsComputer();
    }
}
