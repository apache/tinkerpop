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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;

/**
 * Starts and stops an instance for each executed test.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinServerIntegrationTest {
    private GremlinServer server;
    private final static String epollOption = "gremlin.server.epoll";
    private static final boolean GREMLIN_SERVER_EPOLL = "true".equalsIgnoreCase(System.getProperty(epollOption));
    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinServerIntegrationTest.class);

    @Rule
    public TestName name = new TestName();

    public Settings overrideSettings(final Settings settings) {
        return settings;
    }

    public InputStream getSettingsInputStream() {
        return AbstractGremlinServerIntegrationTest.class.getResourceAsStream("gremlin-server-integration.yaml");
    }

    @Before
    public void setUp() throws Exception {
        logger.info("* Testing: " + name.getMethodName());
        logger.info("* Epoll option enabled:" + GREMLIN_SERVER_EPOLL);

        final InputStream stream = getSettingsInputStream();
        final Settings settings = Settings.read(stream);
        final Settings overridenSettings = overrideSettings(settings);
        ServerTestHelper.rewritePathsInGremlinServerSettings(overridenSettings);
        if (GREMLIN_SERVER_EPOLL) {
            overridenSettings.useEpollEventLoop = true;
        }

        this.server = new GremlinServer(overridenSettings);

        server.start().join();
    }

    @After
    public void tearDown() throws Exception {
        stopServer();
    }

    public void stopServer() throws Exception {
        server.stop().join();
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

    protected static void assumeNeo4jIsPresent() {
        boolean neo4jIncludedForTesting;
        try {
            Class.forName("org.neo4j.tinkerpop.api.impl.Neo4jGraphAPIImpl");
            neo4jIncludedForTesting = true;
        } catch (Exception ex) {
            neo4jIncludedForTesting = false;
        }
        assumeThat("Neo4j implementation was not included for testing - run with -DincludeNeo4j", neo4jIncludedForTesting, is(true));
    }
}
