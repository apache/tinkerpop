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
package org.apache.tinkerpop.gremlin.features;

import io.cucumber.java.Scenario;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;

/**
 * This interface provides the context the test suite needs in order to execute the Gherkin tests. It is implemented
 * by graph providers who wish to test their graph systems against the TinkerPop test suite. It is paired with a
 * test that uses the Cucumber test runner (i.e. {@code @RunWith(Cucumber.class)}) and requires a dependency injection
 * package (e.g. {@code guice}) to push an instance into the Cucumber execution.
 */
public interface World {

    /**
     * Gets a {@link GraphTraversalSource} that is backed by the specified {@link GraphData}. For {@code null}, the
     * returned source should be an empty graph with no data in it. Tests do not mutate the standard graphs. Only tests
     * that use an empty graph will change its state.
     */
    public GraphTraversalSource getGraphTraversalSource(final GraphData graphData);

    /**
     * Called before each individual test is executed which provides an opportunity to do some setup. For example,
     * if there is a specific test that can't be supported it can be ignored by checking for the name with
     * {@code scenario.getName()} and then throwing an {@code AssumptionViolationException}.
     * @param scenario
     */
    public default void beforeEachScenario(final Scenario scenario) {
        // do nothing
    }

    /**
     * Called after each individual test is executed allowing for cleanup of any open resources.
     */
    public default void afterEachScenario() {
        // do nothing
    }
}
