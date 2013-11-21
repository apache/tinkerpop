package com.tinkerpop.gremlin.server;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Optional;

/**
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
        final Optional<Settings> settings = Settings.read(stream);
        if (settings.isPresent()) {
            thread = new Thread(() -> {
                try {
                    new GremlinServer(settings.get()).run();
                } catch (InterruptedException ie) {
                    logger.info("Shutting down Gremlin Server");
                } catch (Exception ex) {
                    logger.error("Could not start Gremlin Server for performance tests", ex);
                }
            });
            thread.start();

            // make sure gremlin server gets off the ground
            Thread.sleep(3000);
        } else
            logger.error("Configuration file at gremlin-server-performance.yaml could not be found or parsed properly.");

        host = System.getProperty("host", "localhost");
        port = System.getProperty("port", "8182");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        Thread.sleep(3000);

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
