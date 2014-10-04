package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.groovy.util.SugarTestHelper;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyCyclicPathTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyDedupTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyExceptTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyFilterTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyHasNotTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyHasTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyIntervalTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyRandomTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyRangeTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyRetainTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovySimplePathTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyWhereTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyBackTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyChooseTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyFoldTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyHiddenValueMapTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyJumpTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyMapTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyMatchTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyOrderByTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyOrderTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyPathTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovySelectTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyUnfoldTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyUntilTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyValueMapTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyVertexTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyAggregateTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyCountTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupByTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupCountTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyInjectTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovySideEffectCapTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyStoreTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyTreeTestImpl;
import com.tinkerpop.gremlin.process.graph.step.util.TraversalSideEffectsTestImpl;
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
            // filter
            GroovyCyclicPathTestImpl.StandardTestImpl.class,
            GroovyDedupTestImpl.StandardTestImpl.class,
            GroovyExceptTestImpl.StandardTestImpl.class,
            GroovyFilterTestImpl.StandardTestImpl.class,
            GroovyHasNotTestImpl.StandardTestImpl.class,
            GroovyHasTestImpl.StandardTestImpl.class,
            GroovyIntervalTestImpl.StandardTestImpl.class,
            GroovyRandomTestImpl.StandardTestImpl.class,
            GroovyRangeTestImpl.StandardTestImpl.class,
            GroovyRetainTestImpl.StandardTestImpl.class,
            GroovySimplePathTestImpl.StandardTestImpl.class,
            GroovyWhereTestImpl.StandardTestImpl.class,
            // map
            GroovyBackTestImpl.StandardTestImpl.class,
            GroovyChooseTestImpl.StandardTestImpl.class,
            GroovyFoldTestImpl.StandardTestImpl.class,
            GroovyHiddenValueMapTestImpl.StandardTestImpl.class,
            GroovyJumpTestImpl.StandardTestImpl.class,
            GroovyMapTestImpl.StandardTestImpl.class,
            GroovyMatchTestImpl.StandardTestImpl.class,
            GroovyOrderByTestImpl.StandardTestImpl.class,
            GroovyOrderTestImpl.StandardTestImpl.class,
            GroovyPathTestImpl.StandardTestImpl.class,
            GroovySelectTestImpl.StandardTestImpl.class,
            GroovyUnfoldTestImpl.StandardTestImpl.class,
            GroovyUntilTestImpl.StandardTestImpl.class,
            GroovyValueMapTestImpl.StandardTestImpl.class,
            GroovyVertexTestImpl.StandardTestImpl.class,
            // sideEffect
            GroovyAggregateTestImpl.StandardTestImpl.class,
            GroovyCountTestImpl.StandardTestImpl.class,
            GroovyGroupByTestImpl.StandardTestImpl.class,
            GroovyGroupCountTestImpl.StandardTestImpl.class,
            GroovyInjectTestImpl.StandardTestImpl.class,
            GroovySideEffectCapTestImpl.StandardTestImpl.class,
            GroovyStoreTestImpl.StandardTestImpl.class,
            GroovyTreeTestImpl.StandardTestImpl.class,

            // util
            TraversalSideEffectsTestImpl.StandardTestImpl.class,

            // compliance
            TraversalCoverageTestImpl.class,
            CoreTraversalTestImpl.class,
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
