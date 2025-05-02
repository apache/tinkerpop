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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SubgraphStrategyConfigTest {

    @Parameterized.Parameter(0)
    public SubgraphStrategy expectedStrategy;

    @Parameterized.Parameters(name = "expectedStrategy={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {SubgraphStrategy.build().vertices(__.has("name", "marko")).create()},
                {SubgraphStrategy.build().edges(__.hasLabel("knows")).create()},
                {SubgraphStrategy.build().vertexProperties(__.hasKey("age")).create()},
                {SubgraphStrategy.build().vertices(__.has("name", "stephen")).edges(__.hasLabel("created")).create()},
        });
    }

    @Test
    public void shouldRoundTripConfiguration() {
        // Get the configuration from the expected strategy
        final Configuration configuration = expectedStrategy.getConfiguration();

        // Recreate the strategy from the configuration
        final SubgraphStrategy recreatedStrategy = SubgraphStrategy.create(configuration);

        // Assert that the recreated strategy matches the expected strategy
        assertEquals(expectedStrategy.getVertexCriterion(), recreatedStrategy.getVertexCriterion());
        assertEquals(expectedStrategy.getEdgeCriterion(), recreatedStrategy.getEdgeCriterion());
        assertEquals(expectedStrategy.getVertexPropertyCriterion(), recreatedStrategy.getVertexPropertyCriterion());
    }
}