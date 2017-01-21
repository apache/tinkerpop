/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

/**
 * Starts and stops one instance for all tests that extend from this class.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinServerPerformanceTest {
    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinServerPerformanceTest.class);

    private static String host;
    private static String port;

    private static CountDownLatch latchWaitForTestsToComplete = new CountDownLatch(1);

    @BeforeClass
    public static void setUp() throws Exception {
        final InputStream stream = AbstractGremlinServerPerformanceTest.class.getResourceAsStream("gremlin-server-performance.yaml");
        final Settings settings = Settings.read(stream);
        ServerTestHelper.rewritePathsInGremlinServerSettings(settings);
        final CompletableFuture<Void> serverReadyFuture = new CompletableFuture<>();

        new Thread(() -> {
            GremlinServer gremlinServer = null;
            try {
                gremlinServer = new GremlinServer(settings);
                gremlinServer.start().join();

                // the server was started and is ready for tests
                serverReadyFuture.complete(null);

                logger.info("Waiting for performance tests to complete...");
                latchWaitForTestsToComplete.await();
            } catch (InterruptedException ie) {
                logger.info("Shutting down Gremlin Server");
            } catch (Exception ex) {
                logger.error("Could not start Gremlin Server for performance tests.", ex);
            } finally {
                logger.info("Tests are complete - prepare to stop Gremlin Server.");
                // reset the wait at this point
                latchWaitForTestsToComplete = new CountDownLatch(1);
                try {
                    if (gremlinServer != null) gremlinServer.stop().join();
                } catch (Exception ex) {
                    logger.error("Could not stop Gremlin Server for performance tests", ex);
                }
            }
        }, "performance-test-server-startup").start();

        // block until gremlin server gets off the ground
        logger.info("Performance test waiting for server to start up");
        serverReadyFuture.join();
        logger.info("Gremlin Server is started and ready for performance test to execute");

        host = System.getProperty("host", "localhost");
        port = System.getProperty("port", "45940");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        latchWaitForTestsToComplete.countDown();
    }

    protected static String getHostPort() {
        return host + ":" + port;
    }

    protected static String getWebSocketBaseUri() {
        return "ws://" + getHostPort() + "/gremlin";
    }
}