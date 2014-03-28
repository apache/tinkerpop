package com.tinkerpop.gremlin.server;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Optional;

/**
 * Starts and stops an instance for each executed test.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinServerIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinServerIntegrationTest.class);

    private Thread thread;
    private String host;
    private String port;

    public Settings overrideSettings(final Settings settings) {
        return settings;
    }

    public InputStream getSettingsInputStream() {
        return AbstractGremlinServerIntegrationTest.class.getResourceAsStream("gremlin-server-integration.yaml");
    }

    @Before
    public void setUp() throws Exception {
        final InputStream stream = getSettingsInputStream();
        final Settings settings = Settings.read(stream);

        thread = new Thread(() -> {
            try {
                final Settings overridenSettings = overrideSettings(settings);
                new GremlinServer(overridenSettings).run();
            } catch (InterruptedException ie) {
                logger.info("Shutting down Gremlin Server");
            } catch (Exception ex) {
                logger.error("Could not start Gremlin Server for integration tests", ex);
            }
        });
        thread.start();

        // make sure gremlin server gets off the ground
        Thread.sleep(1500);

        host = System.getProperty("host", "localhost");
        port = System.getProperty("port", "8182");
    }

    @After
    public void tearDown() throws Exception {
        thread.interrupt();
        while (thread.isAlive()) {
            Thread.sleep(250);
        }
    }

    protected String getHostPort() {
        return host + ":" + port;
    }

    protected String getWebSocketBaseUri() {
        return "ws://" + getHostPort() + "/gremlin";
    }
}
