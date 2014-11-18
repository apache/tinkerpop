package com.tinkerpop.gremlin.server;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Starts and stops one instance for all tests that extend from this class.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinServerPerformanceTest {
    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinServerPerformanceTest.class);

    private static Thread thread;
    private static String host;
    private static String port;

    @BeforeClass
    public static void setUp() throws Exception {
        final InputStream stream = AbstractGremlinServerPerformanceTest.class.getResourceAsStream("gremlin-server-performance.yaml");
        final Settings settings = Settings.read(stream);
        final CompletableFuture<Void> serverReadyFuture = new CompletableFuture<>();

        thread = new Thread(() -> {
            try {
                new GremlinServer(settings, serverReadyFuture).run();
            } catch (InterruptedException ie) {
                logger.info("Shutting down Gremlin Server");
            } catch (Exception ex) {
                logger.error("Could not start Gremlin Server for performance tests", ex);
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

        host = System.getProperty("host", "localhost");
        port = System.getProperty("port", "8182");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        stopServer();
    }

    public static void stopServer() throws Exception {
        if (!thread.isInterrupted())
            thread.interrupt();

        while (thread.isAlive()) {
            Thread.sleep(250);
        }
    }

    protected static String getHostPort() {
        return host + ":" + port;
    }

    protected static String getWebSocketBaseUri() {
        return "ws://" + getHostPort() + "/gremlin";
    }
}