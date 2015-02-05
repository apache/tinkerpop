package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.groovy.util.SugarTestHelper;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.GroovyBranchTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.GroovyChooseTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.GroovyLocalTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.GroovyRepeatTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.GroovyUnionTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovyAndTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovyCoinTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovyCyclicPathTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovyDedupTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovyExceptTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovyFilterTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovyHasNotTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovyHasTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovyIsTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovyOrTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovyRangeTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovyRetainTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovySampleTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovySimplePathTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.GroovyWhereTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyBackTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyCountTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyFoldTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyMapTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyMatchTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyMaxTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyMeanTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyMinTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyOrderTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyPathTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyPropertiesTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovySelectTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyUnfoldTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyValueMapTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.GroovyVertexTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroovyAddEdgeTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroovyAggregateTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroovyGroupCountTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroovyGroupTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroovyInjectTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroovyProfileTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroovySackTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroovySideEffectCapTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroovySideEffectTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroovyStoreTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroovySubgraphTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroovyTreeTest;
import com.tinkerpop.gremlin.process.traversal.CoreTraversalTest;
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
            GroovyBranchTest.StandardTest.class,
            GroovyChooseTest.StandardTest.class,
            GroovyLocalTest.StandardTest.class,
            GroovyRepeatTest.StandardTest.class,
            GroovyUnionTest.StandardTest.class,
            // filter
            GroovyAndTest.StandardTest.class,
            GroovyCoinTest.StandardTest.class,
            GroovyCyclicPathTest.StandardTest.class,
            GroovyDedupTest.StandardTest.class,
            GroovyExceptTest.StandardTest.class,
            GroovyFilterTest.StandardTest.class,
            GroovyHasNotTest.StandardTest.class,
            GroovyHasTest.StandardTest.class,
            GroovyIsTest.StandardTest.class,
            GroovyOrTest.StandardTest.class,
            GroovyRangeTest.StandardTest.class,
            GroovyRetainTest.StandardTest.class,
            GroovySampleTest.StandardTest.class,
            GroovySimplePathTest.StandardTest.class,
            GroovyWhereTest.StandardTest.class,
            // map
            GroovyBackTest.StandardTest.class,
            GroovyCountTest.StandardTest.class,
            GroovyFoldTest.StandardTest.class,
            GroovyMapTest.StandardTest.class,
            GroovyMatchTest.StandardTest.class,
            GroovyMaxTest.StandardTest.class,
            GroovyMeanTest.StandardTest.class,
            GroovyMinTest.StandardTest.class,
            GroovyOrderTest.StandardTest.class,
            GroovyPathTest.StandardTest.class,
            GroovyPropertiesTest.StandardTest.class,
            GroovySelectTest.StandardTest.class,
            GroovyUnfoldTest.StandardTest.class,
            GroovyValueMapTest.StandardTest.class,
            GroovyVertexTest.StandardTest.class,
            // sideEffect
            GroovyAddEdgeTest.StandardTest.class,
            GroovyAggregateTest.StandardTest.class,
            GroovyGroupTest.StandardTest.class,
            GroovyGroupCountTest.StandardTest.class,
            GroovyInjectTest.StandardTest.class,
            GroovyProfileTest.StandardTest.class,
            GroovySackTest.StandardTest.class,
            GroovySideEffectCapTest.StandardTest.class,
            GroovySideEffectTest.StandardTest.class,
            GroovyStoreTest.StandardTest.class,
            GroovySubgraphTest.StandardTest.class,
            GroovyTreeTest.StandardTest.class,

            // compliance
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
