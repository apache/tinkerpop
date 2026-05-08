/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.util.TestSupport;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Validates that each shipped server config can start successfully and serve a basic query.
 * Configs requiring SSL or authentication infrastructure are excluded.
 */
@RunWith(Parameterized.class)
public class GremlinServerConfigIntegrateTest {

    private static final Logger logger = LoggerFactory.getLogger(GremlinServerConfigIntegrateTest.class);

    @Parameterized.Parameter
    public String configName;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<String> configs() {
        return Arrays.asList(
                "gremlin-server.yaml",
                "gremlin-server-min.yaml",
                "gremlin-server-modern.yaml",
                "gremlin-server-classic.yaml",
                "gremlin-server-airroutes.yaml",
                "gremlin-server-modern-readonly.yaml",
                "gremlin-server-rest-modern.yaml",
                "gremlin-server-transaction.yaml"
        );
    }

    @Test
    public void shouldStartAndServeQuery() throws Exception {
        final File confDir = new File(TestSupport.getRootOfBuildDirectory(GremlinServerConfigIntegrateTest.class), "../conf");
        final Settings settings = Settings.read(new FileInputStream(new File(confDir, configName)));

        settings.serializers = Collections.emptyList();
        ServerTestHelper.rewritePathsInGremlinServerSettings(settings);

        final GremlinServer server = new GremlinServer(settings);
        try {
            server.start().join();
            logger.info("Started server with config: {}", configName);

            final Cluster cluster = Cluster.build("localhost").port(settings.port).create();
            final Client client = cluster.connect();
            try {
                final List<Result> results = client.submit("g.inject(1)").all().get();
                assertThat(results.size(), is(1));
                assertThat(results.get(0).getInt(), is(1));
            } finally {
                client.close();
                cluster.close();
            }
        } finally {
            server.getServerGremlinExecutor().getGraphManager().getAsBindings().values().stream()
                    .filter(g -> g instanceof AbstractTinkerGraph)
                    .forEach(g -> ((AbstractTinkerGraph) g).clear());
            server.stop().join();
        }
    }
}
