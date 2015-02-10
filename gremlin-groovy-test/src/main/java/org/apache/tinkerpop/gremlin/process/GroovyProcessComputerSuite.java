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
import org.apache.tinkerpop.gremlin.process.computer.GroovyGraphComputerTest;
import org.apache.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgramTest;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.branch.*;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.*;
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

            GroovyGraphComputerTest.ComputerTest.class,

            //branch
            GroovyBranchTest.ComputerTest.class,
            GroovyChooseTest.ComputerTest.class,
            GroovyLocalTest.ComputerTest.class,
            GroovyRepeatTest.ComputerTest.class,
            GroovyUnionTest.ComputerTest.class,

            // filter
            GroovyAndTest.ComputerTest.class,
            GroovyCoinTest.ComputerTest.class,
            GroovyCyclicPathTest.ComputerTest.class,
            // TODO: GroovyDedupTest.ComputerTest.class
            // TODO: GroovyExceptTest.ComputerTest.class,
            GroovyFilterTest.ComputerTest.class,
            GroovyHasNotTest.ComputerTest.class,
            GroovyHasTest.ComputerTest.class,
            GroovyIsTest.ComputerTest.class,
            GroovyOrTest.ComputerTest.class,
            // TODO: GroovyRangeTest.ComputerTest.class,
            // TODO: GroovyRetainTest.ComputerTest.class,
            GroovySampleTest.ComputerTest.class,
            GroovySimplePathTest.ComputerTest.class,
            GroovyWhereTest.ComputerTest.class,

            // map
            GroovyBackTest.ComputerTest.class,
            GroovyCountTest.ComputerTest.class,
            GroovyFoldTest.ComputerTest.class,
            GroovyMapTest.ComputerTest.class,
            // TODO: GroovyMatchTest.ComputerTest.class,
            GroovyMaxTest.ComputerTest.class,
            GroovyMeanTest.ComputerTest.class,
            GroovyMinTest.ComputerTest.class,
            GroovyOrderTest.ComputerTest.class,
            GroovyPathTest.ComputerTest.class,
            GroovyPropertiesTest.ComputerTest.class,
            GroovySelectTest.ComputerTest.class,
            GroovyUnfoldTest.ComputerTest.class,
            GroovyValueMapTest.ComputerTest.class,
            GroovyVertexTest.ComputerTest.class,
            GroovyCoalesceTest.ComputerTest.class,

            // sideEffect
            // TODO: GroovyAddEdgeTest.ComputerTest.class,
            GroovyAggregateTest.ComputerTest.class,
            GroovyGroupTest.ComputerTest.class,
            GroovyGroupCountTest.ComputerTest.class,
            GroovyInjectTest.ComputerTest.class,
            GroovyProfileTest.ComputerTest.class,
            GroovySackTest.ComputerTest.class,
            GroovySideEffectCapTest.ComputerTest.class,
            // TODO: GroovySideEffectTest.ComputerTest.class,
            GroovyStoreTest.ComputerTest.class,
            // TODO: GroovySubgraphTest.ComputerTest.class,
            GroovyTreeTest.ComputerTest.class,

            // algorithms
            PageRankVertexProgramTest.class,
    };

    public GroovyProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
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
            SugarTestHelper.clearRegistry(GraphManager.get());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
