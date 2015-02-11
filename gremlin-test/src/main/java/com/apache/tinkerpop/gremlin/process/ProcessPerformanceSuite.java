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
package com.apache.tinkerpop.gremlin.process;

import com.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import com.apache.tinkerpop.gremlin.structure.GraphReadPerformanceTest;
import com.apache.tinkerpop.gremlin.structure.GraphWritePerformanceTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The {@code ProcessPerformanceSuite} is a JUnit test runner that executes the Gremlin Test Suite over a Graph
 * implementation.  This specialized test suite and runner is for use by Gremlin implementers to test their
 * Graph implementations traversal speeds.
 * <p/>
 * To use the {@code ProcessPerformanceSuite} define a class in a test module.  Simple naming would expect the name of
 * the implementation followed by "ProcessPerformanceTest".  This class should be annotated as follows (note that
 * the "Suite" implements {@link com.apache.tinkerpop.gremlin.GraphProvider} as a convenience only. It could be implemented in
 * a separate class file):
 * <code>
 * @RunWith(ProcessPerformanceSuite.class)
 * @SProcessPerformanceSuite.GraphProviderClass(TinkerGraphProcessPerformanceTest.class)
 * public class TinkerGraphProcessPerformanceTest implements GraphProvider {
 * }
 * </code>
 * Implementing {@link com.apache.tinkerpop.gremlin.GraphProvider} provides a way for the {@code ProcessPerformanceSuite} to
 * instantiate {@link com.apache.tinkerpop.gremlin.structure.Graph} instances from the implementation being tested to inject
 * into tests in the suite.  The {@code ProcessPerformanceSuite} will utilized
 * {@link com.apache.tinkerpop.gremlin.structure.Graph.Features} defined in the suite to determine which tests will be
 * executed.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProcessPerformanceSuite extends AbstractGremlinSuite {
    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            TraversalPerformanceTest.class
    };

    public ProcessPerformanceSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute);
    }


}
