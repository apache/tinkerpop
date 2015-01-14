package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.groovy.util.SugarTestHelper;
import com.tinkerpop.gremlin.process.computer.GroovyGraphComputerTest;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgramTest;
import com.tinkerpop.gremlin.process.graph.step.branch.GroovyBranchTest;
import com.tinkerpop.gremlin.process.graph.step.branch.GroovyChooseTest;
import com.tinkerpop.gremlin.process.graph.step.branch.GroovyRepeatTest;
import com.tinkerpop.gremlin.process.graph.step.branch.GroovyUnionTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyBetweenTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyCoinTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyCyclicPathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyFilterTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyHasNotTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyHasTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovySampleTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovySimplePathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyWhereTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyBackTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyFoldTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyLocalTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyOrderTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyPathTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyPropertiesTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovySelectTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyShuffleTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyUnfoldTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyValueMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyVertexTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyAggregateTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyCountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupCountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyInjectTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyProfileTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovySackTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovySideEffectCapTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyStoreTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyTreeTest;
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
            GroovyRepeatTest.ComputerTest.class,
            GroovyUnionTest.ComputerTest.class,

            // filter
            GroovyBetweenTest.ComputerTest.class,
            GroovyCyclicPathTest.ComputerTest.class,
            // TODO: GroovyDedupTest.ComputerTest.class
            // TODO: GroovyExceptTest.ComputerTest.class,
            GroovyFilterTest.ComputerTest.class,
            GroovyHasNotTest.ComputerTest.class,
            GroovyHasTest.ComputerTest.class,
            GroovyCoinTest.ComputerTest.class,
            // TODO: GroovyRangeTest.ComputerTest.class,
            // TODO: GroovyRetainTest.ComputerTest.class,
            GroovySampleTest.ComputerTest.class,
            GroovySimplePathTest.ComputerTest.class,
            GroovyWhereTest.ComputerTest.class,

            // map
            GroovyBackTest.ComputerTest.class,
            GroovyFoldTest.ComputerTest.class,
            GroovyLocalTest.ComputerTest.class,
            GroovyMapTest.ComputerTest.class,
            // TODO: GroovyMatchTest.ComputerTest.class,
            // TODO: GroovyOrderByTest.ComputerTest.class,
            GroovyOrderTest.ComputerTest.class,
            GroovyPathTest.ComputerTest.class,
            GroovyPropertiesTest.ComputerTest.class,
            GroovySelectTest.ComputerTest.class,
            GroovyShuffleTest.ComputerTest.class,
            GroovyUnfoldTest.ComputerTest.class,
            GroovyValueMapTest.ComputerTest.class,
            GroovyVertexTest.ComputerTest.class,

            // sideEffect
            // TODO: GroovyAddEdgeTest.ComputerTest.class,
            GroovyAggregateTest.ComputerTest.class,
            GroovyCountTest.ComputerTest.class,
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

            // compliance
            GraphTraversalCoverageTest.class
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
