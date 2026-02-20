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
package org.apache.tinkerpop.gremlin.driver.remote;

import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.ServerTestHelper;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class that contains the setup and teardown for running feature tests.
 */
public abstract class AbstractFeatureTest {
    private static GremlinServer server;

    @BeforeClass
    public static void setUp() throws Exception {
        var filePath = "classpath:" + GremlinServer.class.getPackageName().replace(".", "/") + "/gremlin-server-integration.yaml";
        final Settings settings = Settings.read(filePath);

        ServerTestHelper.rewritePathsInGremlinServerSettings(settings);
        server = new GremlinServer(settings);
        server.start().get(100, TimeUnit.SECONDS);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        server.stop().get(100, TimeUnit.SECONDS);
    }
}
