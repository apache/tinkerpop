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
package org.apache.tinkerpop.gremlin.process;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.groovy.util.SugarTestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest;
import org.apache.tinkerpop.gremlin.process.traversal.GroovyTraversalSideEffectsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.*;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The {@code GroovyProcessStandardSuite} is a JUnit test runner that executes the Gremlin Test Suite over a
 * {@link Graph} implementation.  This test suite covers traversal operations and should be implemented by vendors
 * to validate that their implementations are compliant with the Groovy flavor of the Gremlin language.
 * <br/>
 * For more information on the usage of this suite, please see {@link StructureStandardSuite}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyProcessStandardSuite extends ProcessStandardSuite {

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] allTests = new Class<?>[]{
            // branch
            GroovyBranchTest.StandardTraversals.class,
            GroovyChooseTest.StandardTraversals.class,
            GroovyLocalTest.StandardTraversals.class,
            GroovyRepeatTest.StandardTraversals.class,
            GroovyUnionTest.StandardTraversals.class,
            // filter
            GroovyAndTest.StandardTraversals.class,
            GroovyCoinTest.StandardTraversals.class,
            GroovyCyclicPathTest.StandardTraversals.class,
            GroovyDedupTest.StandardTraversals.class,
            GroovyDropTest.StandardTraversals.class,
            GroovyExceptTest.StandardTraversals.class,
            GroovyFilterTest.StandardTraversals.class,
            GroovyHasNotTest.StandardTraversals.class,
            GroovyHasTest.StandardTraversals.class,
            GroovyIsTest.StandardTraversals.class,
            GroovyBetweenTest.StandardTraversals.class,
            GroovyOrTest.StandardTraversals.class,
            GroovyRangeTest.StandardTraversals.class,
            GroovyRetainTest.StandardTraversals.class,
            GroovySampleTest.StandardTraversals.class,
            GroovySimplePathTest.StandardTraversals.class,
            GroovyWhereTest.StandardTraversals.class,
            // map
            GroovyAddEdgeTest.StandardTraversals.class,
            GroovyAddVertexTest.StandardTraversals.class,
            GroovyCoalesceTest.StandardTraversals.class,
            GroovyCountTest.StandardTraversals.class,
            GroovyFoldTest.StandardTraversals.class,
            GroovyMapTest.StandardTraversals.class,
            GroovyMatchTest.StandardTraversals.class,
            GroovyMaxTest.StandardTraversals.class,
            GroovyMeanTest.StandardTraversals.class,
            GroovyMinTest.StandardTraversals.class,
            GroovyOrderTest.StandardTraversals.class,
            GroovyPathTest.StandardTraversals.class,
            GroovyPropertiesTest.StandardTraversals.class,
            GroovySelectTest.StandardTraversals.class,
            GroovySumTest.StandardTraversals.class,
            GroovyUnfoldTest.StandardTraversals.class,
            GroovyValueMapTest.StandardTraversals.class,
            GroovyVertexTest.StandardTraversals.class,
            // sideEffect
            GroovyAggregateTest.StandardTraversals.class,
            GroovyGroupTest.StandardTraversals.class,
            GroovyGroupCountTest.StandardTraversals.class,
            GroovyInjectTest.StandardTraversals.class,
            GroovyProfileTest.StandardTraversals.class,
            GroovySackTest.StandardTraversals.class,
            GroovySideEffectCapTest.StandardTraversals.class,
            GroovySideEffectTest.StandardTraversals.class,
            GroovyStoreTest.StandardTraversals.class,
            GroovySubgraphTest.StandardTraversals.class,
            GroovyTreeTest.StandardTraversals.class,

            // util
            GroovyTraversalSideEffectsTest.StandardTraversals.class,

            // compliance
            CoreTraversalTest.class,
    };

    public GroovyProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests);
    }

    @Override
    public boolean beforeTestExecution(final Class<? extends AbstractGremlinTest> testClass) {
        unloadSugar();
        SugarLoader.load();
        return true;
    }

    @Override
    public void afterTestExecution(final Class<? extends AbstractGremlinTest> testClass) {
        unloadSugar();
    }

    private void unloadSugar() {
        try {
            SugarTestHelper.clearRegistry(GraphManager.getGraphProvider());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
