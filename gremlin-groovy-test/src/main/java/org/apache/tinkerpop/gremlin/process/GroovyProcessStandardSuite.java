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
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.GroovyBranchTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.GroovyChooseTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.GroovyLocalTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.GroovyRepeatTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.GroovyUnionTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyAndTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyCoinTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyCyclicPathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyDedupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyDropTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyFilterTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyHasTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyIsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyOrTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyRangeTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovySampleTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovySimplePathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyTailTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyWhereTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyAddEdgeTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyAddVertexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyCoalesceTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyConstantTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyCountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyFoldTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyGraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyLoopsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyMapKeysTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyMapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyMapValuesTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyMatchTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyMaxTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyMeanTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyMinTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyOrderTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyPathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyPropertiesTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovySelectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovySumTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyUnfoldTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyValueMapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyVertexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyAggregateTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyExplainTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyGroupCountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyGroupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyGroupTestV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyInjectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyProfileTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovySackTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovySideEffectCapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovySideEffectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyStoreTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovySubgraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyTreeTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The {@code GroovyProcessStandardSuite} is a JUnit test runner that executes the Gremlin Test Suite over a
 * {@link Graph} implementation.  This test suite covers traversal operations and should be implemented by vendors
 * to validate that their implementations are compliant with the Groovy flavor of the Gremlin language.
 * <p/>
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
            GroovyBranchTest.Traversals.class,
            GroovyChooseTest.Traversals.class,
            GroovyLocalTest.Traversals.class,
            GroovyRepeatTest.Traversals.class,
            GroovyUnionTest.Traversals.class,
            // filter
            GroovyAndTest.Traversals.class,
            GroovyCoinTest.Traversals.class,
            GroovyCyclicPathTest.Traversals.class,
            GroovyDedupTest.Traversals.class,
            GroovyDropTest.Traversals.class,
            GroovyFilterTest.Traversals.class,
            GroovyHasTest.Traversals.class,
            GroovyIsTest.Traversals.class,
            GroovyOrTest.Traversals.class,
            GroovyRangeTest.Traversals.class,
            GroovySampleTest.Traversals.class,
            GroovySimplePathTest.Traversals.class,
            GroovyTailTest.Traversals.class,
            GroovyWhereTest.Traversals.class,
            // map
            GroovyAddEdgeTest.Traversals.class,
            GroovyAddVertexTest.Traversals.class,
            GroovyCoalesceTest.Traversals.class,
            GroovyConstantTest.Traversals.class,
            GroovyCountTest.Traversals.class,
            GroovyFoldTest.Traversals.class,
            GroovyGraphTest.Traversals.class,
            GroovyLoopsTest.Traversals.class,
            GroovyMapTest.Traversals.class,
            GroovyMapKeysTest.Traversals.class,
            GroovyMapValuesTest.Traversals.class,
            GroovyMatchTest.CountMatchTraversals.class,
            GroovyMatchTest.GreedyMatchTraversals.class,
            GroovyMaxTest.Traversals.class,
            GroovyMeanTest.Traversals.class,
            GroovyMinTest.Traversals.class,
            GroovyOrderTest.Traversals.class,
            GroovyPathTest.Traversals.class,
            GroovyPropertiesTest.Traversals.class,
            GroovySelectTest.Traversals.class,
            GroovySumTest.Traversals.class,
            GroovyUnfoldTest.Traversals.class,
            GroovyValueMapTest.Traversals.class,
            GroovyVertexTest.Traversals.class,
            // sideEffect
            GroovyAggregateTest.Traversals.class,
            GroovyExplainTest.Traversals.class,
            GroovyGroupTest.Traversals.class,
            GroovyGroupTestV3d0.Traversals.class,
            GroovyGroupCountTest.Traversals.class,
            GroovyInjectTest.Traversals.class,
            GroovyProfileTest.Traversals.class,
            GroovySackTest.Traversals.class,
            GroovySideEffectCapTest.Traversals.class,
            GroovySideEffectTest.Traversals.class,
            GroovyStoreTest.Traversals.class,
            GroovySubgraphTest.Traversals.class,
            GroovyTreeTest.Traversals.class,

            // util
            GroovyTraversalSideEffectsTest.Traversals.class,

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
