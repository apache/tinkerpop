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
package ${package};

import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;

import java.io.InputStream;
import java.util.List;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

/**
 * Basic test class that demonstrates how to start and stop an embedded Gremlin Server instance in a test. Note that
 * the server instance is not started or stopped in a thread-safe manner, but typically this is acceptable for most
 * testing use cases.
 */
public class ServiceTest {
    private GremlinServer server;

    private static Service service = Service.instance();

    @Before
    public void setUp() throws Exception {
        startServer();
    }

    /**
     * Starts a new instance of Gremlin Server.
     */
    public void startServer() throws Exception {
        final InputStream stream = ServiceTest.class.getResourceAsStream("gremlin-server.yaml");
        this.server = new GremlinServer(Settings.read(stream));

        server.start().join();
    }

    @After
    public void tearDown() throws Exception {
        stopServer();
    }

    /**
     * Stops a current instance of Gremlin Server.
     */
    public void stopServer() throws Exception {
        server.stop().join();
    }

    @AfterClass
    public static void tearDownCase() throws Exception {
        service.close();
    }

    @Test
    public void shouldCreateGraph() throws Exception {
        final List<Object> result = service.findCreatorsOfSoftware("lop");
        assertThat(result, is(Arrays.asList("marko", "josh", "peter")));
    }
}