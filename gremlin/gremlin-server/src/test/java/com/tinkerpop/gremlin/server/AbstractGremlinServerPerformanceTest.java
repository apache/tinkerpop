package com.tinkerpop.gremlin.server;

import java.io.InputStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinServerPerformanceTest extends AbsstractServerInstanceTest {
    @Override
    public InputStream getSettingsInputStream() {
        return AbstractGremlinServerPerformanceTest.class.getResourceAsStream("gremlin-server-performance.yaml");
    }
}
