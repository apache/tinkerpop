package com.tinkerpop.gremlin.server;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbsstractServerInstanceTest {
    private static final Logger logger = LoggerFactory.getLogger(AbsstractServerInstanceTest.class);

    private Thread thread;
    private String host;
    private String port;

    public Settings overrideSettings(final Settings settings) {
        return settings;
    }

    public abstract InputStream getSettingsInputStream();

    @Before
    public void setUp() throws Exception {
        final InputStream stream = getSettingsInputStream();
        final Optional<Settings> settings = Settings.read(stream);
        if (settings.isPresent()) {
            thread = new Thread(() -> {
                try {
                    final Settings overridenSettings = overrideSettings(settings.get());
                    new GremlinServer(overridenSettings).run();
                } catch (InterruptedException ie) {
                    logger.info("Shutting down Gremlin Server");
                } catch (Exception ex) {
                    logger.error("Could not start Gremlin Server for integration tests", ex);
                }
            });
            thread.start();

            // make sure gremlin server gets off the ground
            Thread.sleep(500);
        } else
            logger.error("Configuration file at gremlin-server-integration.yaml could not be found or parsed properly.");

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
