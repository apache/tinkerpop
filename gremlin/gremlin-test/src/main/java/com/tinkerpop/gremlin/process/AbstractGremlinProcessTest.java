package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinTest;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinProcessTest extends AbstractGremlinTest {
    protected boolean requiresGraphComputer;

    protected boolean graphMeetsTestRequirements() {
        return !requiresGraphComputer || g.getFeatures().graph().supportsComputer();
    }
}
