package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.groovy.util.SugarTestHelper;
import com.tinkerpop.gremlin.process.computer.GroovyGraphComputerTest;
import com.tinkerpop.gremlin.process.graph.step.branch.GroovyChooseTest;
import com.tinkerpop.gremlin.process.graph.step.branch.GroovyJumpTest;
import com.tinkerpop.gremlin.process.graph.step.branch.GroovyUnionTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyCyclicPathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyFilterTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyHasNotTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyHasTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyIntervalTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyRandomTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovySimplePathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyWhereTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyBackTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyHiddenValueMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyPathTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyPropertiesTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovySelectTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyShuffleTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyUnfoldTest;
import com.tinkerpop.gremlin.process.graph.step.branch.GroovyUntilTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyValueMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyVertexTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyAggregateTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyCountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupByTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupCountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyInjectTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyProfileTest;
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
            GroovyChooseTest.ComputerTest.class,
            GroovyJumpTest.ComputerTest.class,
            GroovyUnionTest.ComputerTest.class,
            GroovyUntilTest.ComputerTest.class,

            // filter
            GroovyCyclicPathTest.ComputerTest.class,
            // TODO: GroovyDedupTest.ComputerTest.class
            // TODO: GroovyExceptTest.ComputerTest.class,
            GroovyFilterTest.ComputerTest.class,
            GroovyHasNotTest.ComputerTest.class,
            GroovyHasTest.ComputerTest.class,
            GroovyIntervalTest.ComputerTest.class,
            GroovyRandomTest.ComputerTest.class,
            // TODO: GroovyRangeTest.ComputerTest.class,
            // TODO: GroovyRetainTest.ComputerTest.class,
            GroovySimplePathTest.ComputerTest.class,
            GroovyWhereTest.ComputerTest.class,

            // map
            GroovyBackTest.ComputerTest.class,
            // TODO: GroovyChooseTest.ComputerTest.class,
            // TODO: GroovyFoldTest.ComputerTest.class,
            GroovyHiddenValueMapTest.ComputerTest.class,

            GroovyMapTest.ComputerTest.class,
            // TODO: GroovyMatchTest.ComputerTest.class,
            // TODO: GroovyOrderByTest.ComputerTest.class,
            // TODO: GroovyOrderTest.ComputerTest.class,
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
            GroovyGroupByTest.ComputerTest.class,
            GroovyGroupCountTest.ComputerTest.class,
            GroovyInjectTest.ComputerTest.class,
            GroovyProfileTest.ComputerTest.class,
            GroovySideEffectCapTest.ComputerTest.class,
            GroovyStoreTest.ComputerTest.class,
            GroovyTreeTest.ComputerTest.class
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
