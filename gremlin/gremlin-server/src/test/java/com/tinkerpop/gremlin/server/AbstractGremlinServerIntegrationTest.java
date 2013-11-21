package com.tinkerpop.gremlin.server;

import java.io.InputStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinServerIntegrationTest extends AbsstractServerInstanceTest {
    @Override
    public InputStream getSettingsInputStream() {
        return AbstractGremlinServerIntegrationTest.class.getResourceAsStream("gremlin-server-integration.yaml");
    }
}
