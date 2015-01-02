package com.tinkerpop.gremlin.console.groovy.plugin;

import com.tinkerpop.gremlin.server.GremlinServer;
import com.tinkerpop.gremlin.server.Settings;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Starts and stops an instance for each executed test.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinServerIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinServerIntegrationTest.class);

    private Thread thread;

    private GremlinServer server;

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
        final CompletableFuture<Void> serverReadyFuture = new CompletableFuture<>();

        final Settings overridenSettings = overrideSettings(settings);
        final GremlinServer server = new GremlinServer(overridenSettings, serverReadyFuture);
        this.server = server;

        thread = new Thread(() -> {
            try {
                server.run();
            } catch (InterruptedException ie) {
                logger.info("Shutting down Gremlin Server");
            } catch (Exception ex) {
                logger.error("Could not start Gremlin Server for integration tests", ex);
            }
        });
        thread.start();

        // make sure gremlin server gets off the ground - longer than 30 seconds means that this didn't work somehow
        try {
            serverReadyFuture.get(30000, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            logger.error("Server did not start in the expected time or was otherwise interrupted.", ex);
            return;
        }
    }

    @After
    public void tearDown() throws Exception {
        stopServer();
    }

    public void stopServer() throws Exception {
        server.stop();

        if (!thread.isInterrupted())
            thread.interrupt();

        while (thread.isAlive()) {
            Thread.sleep(250);
        }
    }
}
