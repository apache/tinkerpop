package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.groovy.util.SugarTestHelper;
import com.tinkerpop.gremlin.process.graph.step.branch.GroovyChooseTest;
import com.tinkerpop.gremlin.process.graph.step.branch.GroovyJumpTest;
import com.tinkerpop.gremlin.process.graph.step.branch.GroovyUnionTest;
import com.tinkerpop.gremlin.process.graph.step.branch.GroovyUntilTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyCyclicPathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyDedupTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyExceptTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyFilterTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyHasNotTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyHasTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyIntervalTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyRandomTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyRangeTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyRetainTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovySimplePathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyWhereTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyBackTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyFoldTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyHiddenValueMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyMatchTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyOrderByTest;
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
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupByTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupCountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyInjectTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyProfileTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovySideEffectCapTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovySideEffectTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyStoreTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovySubgraphTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyTreeTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The test suite for the Groovy implementation of Gremlin Process.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyProcessStandardSuite extends ProcessStandardSuite {

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            // branch
            GroovyChooseTest.StandardTest.class,
            GroovyJumpTest.StandardTest.class,
            GroovyUnionTest.StandardTest.class,
            GroovyUntilTest.StandardTest.class,
            // filter
            GroovyCyclicPathTest.StandardTest.class,
            GroovyDedupTest.StandardTest.class,
            GroovyExceptTest.StandardTest.class,
            GroovyFilterTest.StandardTest.class,
            GroovyHasNotTest.StandardTest.class,
            GroovyHasTest.StandardTest.class,
            GroovyIntervalTest.StandardTest.class,
            GroovyRandomTest.StandardTest.class,
            GroovyRangeTest.StandardTest.class,
            GroovyRetainTest.StandardTest.class,
            GroovySimplePathTest.StandardTest.class,
            GroovyWhereTest.StandardTest.class,
            // map
            GroovyBackTest.StandardTest.class,
            GroovyFoldTest.StandardTest.class,
            GroovyHiddenValueMapTest.StandardTest.class,
            GroovyMapTest.StandardTest.class,
            GroovyMatchTest.StandardTest.class,
            GroovyOrderByTest.StandardTest.class,
            GroovyOrderTest.StandardTest.class,
            GroovyPathTest.StandardTest.class,
            GroovyPropertiesTest.StandardTest.class,
            GroovySelectTest.StandardTest.class,
            GroovyShuffleTest.StandardTest.class,
            GroovyUnfoldTest.StandardTest.class,
            GroovyValueMapTest.StandardTest.class,
            GroovyVertexTest.StandardTest.class,
            // sideEffect
            GroovyAggregateTest.StandardTest.class,
            GroovyCountTest.StandardTest.class,
            GroovyGroupByTest.StandardTest.class,
            GroovyGroupCountTest.StandardTest.class,
            GroovyInjectTest.StandardTest.class,
            GroovyProfileTest.StandardTest.class,
            GroovySideEffectCapTest.StandardTest.class,
            GroovySideEffectTest.StandardTest.class,
            GroovyStoreTest.StandardTest.class,
            GroovySubgraphTest.StandardTest.class,
            GroovyTreeTest.StandardTest.class,

            // util
            // TraversalSideEffectsTest.StandardTest.class,

            // compliance
            TraversalCoverageTest.class,
            CoreTraversalTest.class,
    };


    public GroovyProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce, true);
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
