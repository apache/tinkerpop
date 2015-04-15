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
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest;
import org.apache.tinkerpop.gremlin.algorithm.generator.DistributionGeneratorTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdgeTest;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPropertyTest;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexPropertyTest;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexTest;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdgeTest;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexPropertyTest;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexTest;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The {@code StructureStandardSuite} is a JUnit test runner that executes the Gremlin Test Suite over a
 * {@link Graph} implementation.  This specialized test suite and runner is for use by
 * vendors to test their {@link Graph} implementations.  The {@code StructureStandardSuite} ensures consistency and
 * validity of the implementations that they test.  Successful execution of this test suite is critical to proper
 * operations of a vendor implementation.
 * <p/>
 * To use the {@code StructureStandardSuite} define a class in a test module.  Simple naming would expect the name of
 * the implementation followed by "StructureStandardSuite".  This class should be annotated as follows (note that the
 * "Suite" implements {@link GraphProvider} as a convenience only. It could be implemented in a
 * separate class file):
 * <code>
 * @RunWith(StructureStandardSuite.class)
 * @StructureStandardSuite.GraphProviderClass(TinkerGraphStructureStandardTest.class)
 * public class TinkerGraphStructureStandardTest implements GraphProvider {
 * }
 * </code>
 * Implementing {@link GraphProvider} provides a way for the {@code StructureStandardSuite} to instantiate
 * {@link Graph} instances from the implementation being tested to inject into tests in the suite.  The
 * {@code StructureStandardSuite} will utilized Features defined in the suite to determine which tests will be executed.
 * <br/>
 * Set the {@code gremlin.tests} environment variable to a comma separated list of test classes to execute.
 * This setting can be helpful to restrict execution of tests to specific ones being focused on during development.

 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StructureStandardSuite extends AbstractGremlinSuite {

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] allTests = new Class<?>[]{
            BatchTest.class,
            CommunityGeneratorTest.class,
            DetachedEdgeTest.class,
            DetachedVertexPropertyTest.class,
            DetachedPropertyTest.class,
            DetachedVertexTest.class,
            DistributionGeneratorTest.class,
            EdgeTest.class,
            FeatureSupportTest.class,
            GraphTest.class,
            GraphConstructionTest.class,
            IoTest.class,
            VertexPropertyTest.class,
            VariablesTest.class,
            PropertyTest.class,
            ReferenceEdgeTest.class,
            ReferenceVertexPropertyTest.class,
            ReferenceVertexTest.class,
            SerializationTest.class,
            StarGraphTest.class,
            TransactionTest.class,
            VertexTest.class
    };

    public StructureStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, null, false, TraversalEngine.Type.STANDARD);
    }
}
