package com.tinkerpop.gremlin.server;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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

        final Settings overridenSettings = overrideSettings(settings);
        final GremlinServer server = new GremlinServer(overridenSettings);
        this.server = server;

        server.run().join();
    }

    @After
    public void tearDown() throws Exception {
        stopServer();
    }

    public void stopServer() throws Exception {
        server.stop();
    }

    public static boolean deleteDirectory(final File directory) {
        if (directory.exists()) {
            final File[] files = directory.listFiles();
            if (null != files) {
                for (int i = 0; i < files.length; i++) {
                    if (files[i].isDirectory()) {
                        deleteDirectory(files[i]);
                    } else {
                        files[i].delete();
                    }
                }
            }
        }

        return (directory.delete());
    }
}
