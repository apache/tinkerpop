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
package org.apache.tinkerpop.gremlin.driver;

import examples.BasicGremlin;
import examples.Connections;
import examples.ModernTraversals;
import org.apache.tinkerpop.gremlin.server.AbstractGremlinServerIntegrationTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Integration tests for driver examples to ensure they work with the current driver.
 * These tests catch breaking changes that would affect example code.
 */
public class DriverExamplesIntegrateTest extends AbstractGremlinServerIntegrationTest {

    @BeforeClass
    public static void setUpEnvironment() {
        // Set environment variables for examples to connect to test server
        System.setProperty("GREMLIN_SERVER_HOST", "localhost");
        System.setProperty("GREMLIN_SERVER_PORT", "45940");
    }

    @AfterClass
    public static void tearDownEnvironment() {
        // Clean up system properties
        System.clearProperty("GREMLIN_SERVER_HOST");
        System.clearProperty("GREMLIN_SERVER_PORT");
    }

    @Test
    public void shouldRunBasicGremlinExample() throws Exception {
        try {
            BasicGremlin.main(new String[]{});
        } catch (Exception e) {
            fail("BasicGremlin example failed: " + e.getMessage());
        }
    }

    @Test
    public void shouldRunModernTraversalsExample() throws Exception {
        try {
            ModernTraversals.main(new String[]{});
        } catch (Exception e) {
            fail("ModernTraversals example failed: " + e.getMessage());
        }
    }

    @Test
    public void shouldRunConnectionsExample() throws Exception {
        try {
            Connections.main(new String[]{});
        } catch (Exception e) {
            fail("Connections example failed: " + e.getMessage());
        }
    }
}
