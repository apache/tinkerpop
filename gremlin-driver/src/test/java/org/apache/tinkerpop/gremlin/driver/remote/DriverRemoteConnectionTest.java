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

import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.driver.RequestOptions.getRequestOptions;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverRemoteConnectionTest {
    private static final GraphTraversalSource g = EmptyGraph.instance().traversal();

    @Test
    public void shouldBuildRequestOptions() {
        final RequestOptions options = getRequestOptions(
                g.with("x").
                        with("y", 100).
                        with(Tokens.ARGS_BATCH_SIZE, 1000).
                        with(Tokens.ARGS_EVAL_TIMEOUT, 100000L).
                        with(Tokens.ARGS_USER_AGENT, "test").
                        V().asAdmin().getGremlinLang());
        assertEquals(1000, options.getBatchSize().get().intValue());
        assertEquals(100000L, options.getTimeout().get().longValue());
    }

    @Test
    public void shouldBuildRequestOptionsWithNumerics() {
        final RequestOptions options = getRequestOptions(
                g.with(Tokens.ARGS_BATCH_SIZE, 100).
                  with(Tokens.ARGS_EVAL_TIMEOUT, 1000).
                  V().asAdmin().getGremlinLang());
        assertEquals(Integer.valueOf(100), options.getBatchSize().get());
        assertEquals(Long.valueOf(1000), options.getTimeout().get());
    }
}
