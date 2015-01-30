package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.groovy.util.SugarTestHelper;
import com.tinkerpop.gremlin.process.graph.traversal.branch.GroovyBranchTest;
import com.tinkerpop.gremlin.process.graph.traversal.branch.GroovyChooseTest;
import com.tinkerpop.gremlin.process.graph.traversal.branch.GroovyLocalTest;
import com.tinkerpop.gremlin.process.graph.traversal.branch.GroovyRepeatTest;
import com.tinkerpop.gremlin.process.graph.traversal.branch.GroovyUnionTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovyAndTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovyCoinTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovyCyclicPathTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovyDedupTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovyExceptTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovyFilterTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovyHasNotTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovyHasTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovyIsTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovyOrTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovyRangeTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovyRetainTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovySampleTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovySimplePathTest;
import com.tinkerpop.gremlin.process.graph.traversal.filter.GroovyWhereTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyBackTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyCountTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyFoldTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyMapTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyMatchTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyMaxTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyMeanTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyMinTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyOrderTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyPathTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyPropertiesTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovySelectTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyUnfoldTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyValueMapTest;
import com.tinkerpop.gremlin.process.graph.traversal.map.GroovyVertexTest;
import com.tinkerpop.gremlin.process.graph.traversal.sideEffect.GroovyAddEdgeTest;
import com.tinkerpop.gremlin.process.graph.traversal.sideEffect.GroovyAggregateTest;
import com.tinkerpop.gremlin.process.graph.traversal.sideEffect.GroovyGroupCountTest;
import com.tinkerpop.gremlin.process.graph.traversal.sideEffect.GroovyGroupTest;
import com.tinkerpop.gremlin.process.graph.traversal.sideEffect.GroovyInjectTest;
import com.tinkerpop.gremlin.process.graph.traversal.sideEffect.GroovyProfileTest;
import com.tinkerpop.gremlin.process.graph.traversal.sideEffect.GroovySackTest;
import com.tinkerpop.gremlin.process.graph.traversal.sideEffect.GroovySideEffectCapTest;
import com.tinkerpop.gremlin.process.graph.traversal.sideEffect.GroovySideEffectTest;
import com.tinkerpop.gremlin.process.graph.traversal.sideEffect.GroovyStoreTest;
import com.tinkerpop.gremlin.process.graph.traversal.sideEffect.GroovySubgraphTest;
import com.tinkerpop.gremlin.process.graph.traversal.sideEffect.GroovyTreeTest;
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
