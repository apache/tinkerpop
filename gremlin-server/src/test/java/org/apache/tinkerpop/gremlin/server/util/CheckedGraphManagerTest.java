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
package org.apache.tinkerpop.gremlin.server.util;

import static java.util.stream.Collectors.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.util.function.Function;

import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;

import org.junit.Before;
import org.junit.Test;

public class CheckedGraphManagerTest {

    private Settings settings;

    @Before
    public void before() throws Exception {
        settings = Settings
                .read(CheckedGraphManagerTest.class.getResourceAsStream("../gremlin-server-integration.yaml"));
    }

    /**
     * TINKERPOP-2436 The gremlin server starts even if all graphs instantiation
     */
    @Test(expected = IllegalStateException.class)
    public void noGraph() {
        settings.graphs.clear();
        new CheckedGraphManager(settings);
    }

    /**
     * TINKERPOP-2436 The gremlin server starts even if all graphs instantiation
     */
    @Test(expected = IllegalStateException.class)
    public void singleGraphFails() {
        settings.graphs.clear();
        settings.graphs.put("invalid", "conf/invalidPath");
        new CheckedGraphManager(settings);
    }

    /**
     * TINKERPOP-2436 The gremlin server starts even if all graphs instantiation
     */
    @Test
    public void justAGraphFails() {
        settings.graphs.put("invalid", "conf/invalidPath");
        final GraphManager manager = new CheckedGraphManager(settings);
        assertThat(manager.getGraphNames(), hasSize(6));
    }

    /**
     * TINKERPOP-2436 The gremlin server starts even if all graphs instantiation
     */
    @Test(expected = IllegalStateException.class)
    public void allGraphFails() {
        settings.graphs = settings.graphs.keySet().stream()
                .collect(toMap(Function.identity(), name -> "conf/invalidPath"));
        new CheckedGraphManager(settings);
    }

}
