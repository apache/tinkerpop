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
import org.apache.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgramTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.GroovyBranchTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.GroovyChooseTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.GroovyLocalTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.GroovyRepeatTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.GroovyUnionTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyAndTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyCoinTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyCyclicPathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyDedupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyFilterTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyHasNotTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyHasTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyIsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyOrTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyRangeTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovySampleTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovySimplePathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.GroovyWhereTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyBackTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyCoalesceTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyCountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyFoldTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyMapTest;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyGroupCountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyGroupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyInjectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyProfileTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovySackTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovySideEffectCapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyStoreTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyTreeTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyProcessComputerSuite extends ProcessComputerSuite {

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{

            //branch
            GroovyBranchTest.ComputerTraversals.class,
            GroovyChooseTest.ComputerTraversals.class,
            GroovyLocalTest.ComputerTraversals.class,
            GroovyRepeatTest.ComputerTraversals.class,
            GroovyUnionTest.ComputerTraversals.class,

            // filter
            GroovyAndTest.ComputerTraversals.class,
            GroovyCoinTest.ComputerTraversals.class,
            GroovyCyclicPathTest.ComputerTraversals.class,
            GroovyDedupTest.ComputerTraversals.class,
            // TODO: GroovyExceptTest.ComputerTest.class,
            GroovyFilterTest.ComputerTraversals.class,
            GroovyHasNotTest.ComputerTraversals.class,
            GroovyHasTest.ComputerTraversals.class,
            GroovyIsTest.ComputerTraversals.class,
            GroovyOrTest.ComputerTraversals.class,
            GroovyRangeTest.ComputerTraversals.class,
            // TODO: GroovyRetainTest.ComputerTest.class,
            GroovySampleTest.ComputerTraversals.class,
            GroovySimplePathTest.ComputerTraversals.class,
            GroovyWhereTest.ComputerTraversals.class,

            // map
            GroovyBackTest.ComputerTraversals.class,
            GroovyCoalesceTest.ComputerTraversals.class,
            GroovyCountTest.ComputerTraversals.class,
            GroovyFoldTest.ComputerTraversals.class,
            GroovyMapTest.ComputerTraversals.class,
            // TODO: GroovyMatchTest.ComputerTest.class,
            GroovyMaxTest.ComputerTraversals.class,
            GroovyMeanTest.ComputerTraversals.class,
            GroovyMinTest.ComputerTraversals.class,
            GroovyOrderTest.ComputerTraversals.class,
            GroovyPathTest.ComputerTraversals.class,
            GroovyPropertiesTest.ComputerTraversals.class,
            GroovySelectTest.ComputerTraversals.class,
            GroovySumTest.ComputerTraversals.class,
            GroovyUnfoldTest.ComputerTraversals.class,
            GroovyValueMapTest.ComputerTraversals.class,
            GroovyVertexTest.ComputerTraversals.class,


            // sideEffect
            // TODO: GroovyAddEdgeTest.ComputerTest.class,
            GroovyAggregateTest.ComputerTraversals.class,
            GroovyGroupTest.ComputerTraversals.class,
            GroovyGroupCountTest.ComputerTraversals.class,
            GroovyInjectTest.ComputerTraversals.class,
            GroovyProfileTest.ComputerTraversals.class,
            GroovySackTest.ComputerTraversals.class,
            GroovySideEffectCapTest.ComputerTraversals.class,
            // TODO: GroovySideEffectTest.ComputerTest.class,
            GroovyStoreTest.ComputerTraversals.class,
            // TODO: GroovySubgraphTest.ComputerTest.class,
            GroovyTreeTest.ComputerTraversals.class,

            // algorithms
            PageRankVertexProgramTest.class,
    };

    public GroovyProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToExecute);
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
