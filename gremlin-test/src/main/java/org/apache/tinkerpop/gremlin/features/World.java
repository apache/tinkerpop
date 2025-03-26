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
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;

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
     * {@code scenario.getName()} and then throwing an {@code AssumptionViolatedException}.
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

    /**
     * Called when {@code g.io()} is encountered in the Gherkin tests and allows the path to the data file to
     * referenced to be changed. The default path will look something like:  {@code data/file.extension} and will
     * match one of the standard TinkerPop data files associated with the test framework. If the files need to be
     * located somewhere else for a particular provider, this method can alter the path as needed.
     *
     * @param pathToFileFromGremlin the path to a data file as taken from the Gherkin tests
     */
    public default String changePathToDataFile(final String pathToFileFromGremlin) {
        return pathToFileFromGremlin;
    }

    /**
     * Converts a graph element's {@link T#id} to a form that can be used in a script parsed by the grammar. For
     * example, if the graph has numeric identifiers the default implementation of {@code id().toString()} would
     * return "0" which would be interpreted by the grammar as a number when parsed in {@code g.V(0)}. However, a
     * graph that used {@code UUID} for an identifier would have a representation of
     * "1c535978-dc36-4cd2-ab82-95a98a847757" which could not be parsed by the grammar directly as
     * {@code g.V(1c535978-dc36-4cd2-ab82-95a98a847757)} and would need to be prefixed and suffixed with double or
     * single quotes. Therefore, this method would be overridden for that graph to perform that function.
     */
    public default String convertIdToScript(final Object id, final Class<? extends Element> type) {
        return id.toString();
    }

    /**
     * Determines if the test should use parameters literally or treat them as variables to be applied to the script.
     * By default, they are treated literally.
     */
    public default boolean useParametersLiterally() {
        return true;
    }


    /**
     * Allows for some flexibility in error message assertion, where the provider can handle assertions themselves.
     * Providers should use standard assertion logic as they would with tests. Note that if this method is called, then
     * the exception has happened and that the only point of concern for assertion is the message. Providers can not use
     * this method as a way to avoid throwing an exception in the first place. TinkerPop tries to not be too
     * prescriptive with error messages and while we recommend providers conform to our messages it is not required.
     * @return {@code true} if the assertion was handled and {@code false} if default handling should be engaged
     */
    public default boolean handleErrorMessageAssertion(final String comparison, final String expectedMessage,
                                                       final Throwable actualException) {
        return false;
    }
}
