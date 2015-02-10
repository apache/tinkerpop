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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The {@code StructurePerformanceSuite} is a JUnit test runner that executes the Gremlin Test Suite over a
 * {@link org.apache.tinkerpop.gremlin.structure.Graph} implementation.  This specialized test suite and runner is for use
 * by Gremlin Structure implementers to test their {@link org.apache.tinkerpop.gremlin.structure.Graph}implementations.
 * The {@code StructurePerformanceSuite} runs more complex testing scenarios over
 * {@link org.apache.tinkerpop.gremlin.structure.Graph} implementations than the standard {@code StructurePerformanceSuite}.
 * <p/>
 * To use the {@code StructurePerformanceSuite} define a class in a test module.  Simple naming would expect the name
 * of the implementation followed by "StructurePerformanceTest".  This class should be annotated as follows (note that
 * the "Suite" implements {@link org.apache.tinkerpop.gremlin.GraphProvider} as a convenience only. It could be implemented
 * in a separate class file):
 * <code>
 * @RunWith(StructurePerformanceSuite.class)
 * @StructurePerformanceSuite.GraphProviderClass(TinkerGraphStructurePerformanceTest.class) public class TinkerGraphStructurePerformanceTest implements GraphProvider {
 * }
 * </code>
 * Implementing {@link org.apache.tinkerpop.gremlin.GraphProvider} provides a way for the {@code StructurePerformanceSuite}
 * to instantiate {@link org.apache.tinkerpop.gremlin.structure.Graph} instances from the implementation being tested to
 * inject into tests in the suite.  The {@code StructurePerformanceSuite} will utilized
 * {@link org.apache.tinkerpop.gremlin.structure.Graph.Features} defined in the suite to determine which tests will be
 * executed.
 * <br/>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StructurePerformanceSuite extends AbstractGremlinSuite {
    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            GraphWritePerformanceTest.class,
            GraphReadPerformanceTest.class
    };

    public StructurePerformanceSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute);
    }


}
