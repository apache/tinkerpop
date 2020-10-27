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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.server.util.CheckedGraphManager;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.junit.After;
import org.junit.Test;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests to validate shutdown mechanics.
 */
public class GremlinServerShutdownIntegrationTest {

    private GremlinServer server;

    @After
    public void after() {
        if (server != null) {
            server.stop();
        }
    }

    public InputStream getSettingsInputStream() {
        return AbstractGremlinServerIntegrationTest.class.getResourceAsStream("gremlin-server-integration.yaml");
    }

    public Settings getBaseSettings() {
        final InputStream stream = getSettingsInputStream();
        return Settings.read(stream);
    }

    public CompletableFuture<ServerGremlinExecutor> startServer(final Settings settings) throws Exception {
        ServerTestHelper.rewritePathsInGremlinServerSettings(settings);
        server = new GremlinServer(settings);
        return server.start();
    }

    @Test
    public void shouldNotLoadBadChannelizer() {
        final Settings settings = getBaseSettings();
        settings.channelizer = "org.apache.tinkerpop.gremlin.server.ThisChannelizerDoesNotExist*!";

        try {
            startServer(settings).join();
            fail("Server should not have started");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals(ClassNotFoundException.class, root.getClass());
        }
    }

    /**
     * TINKERPOP-2436 The gremlin server starts even if all graphs instantiation has
     * failed
     */
    @Test
    public void failOnInvalidGraphWithCheckedGraphManager() {
        final Settings settings = getBaseSettings();
        settings.graphs.clear();
        settings.graphManager = CheckedGraphManager.class.getName();
        settings.graphs.put("invalid", "conf/invalidPath");

        try {
            startServer(settings).join();
            fail("Server should not have started");
        } catch (Exception ex) {
            assertTrue(ExceptionUtils.getThrowableList(ex).stream().filter(e -> e instanceof IllegalStateException)
                    .anyMatch(e -> e.getMessage().contains("Graph [invalid] configured at")));
        }
    }

    @Test
    public void startOnInvalidGraphWithDefaultGraphManager() throws Exception {
        final Settings settings = getBaseSettings();
        settings.graphs.clear();
        settings.graphs.put("invalid", "conf/invalidPath");
        startServer(settings);
    }
}
