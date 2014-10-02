package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
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
import com.tinkerpop.gremlin.process.graph.step.map.GroovyChooseTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyFoldTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyHiddenValueMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyJumpTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyMatchTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyOrderByTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyOrderTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyPathTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovySelectTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyUnfoldTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyUntilTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyValueMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyVertexTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyAggregateTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyCountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupByTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupCountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyInjectTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovySideEffectCapTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyStoreTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyTreeTest;
import com.tinkerpop.gremlin.process.graph.step.util.TraversalSideEffectsTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The test suite for the Groovy implementation of Gremlin Process.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyProcessStandardSuite extends ProcessStandardSuite {

    static {
        SugarLoader.load();
    }

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            // filter
            GroovyCyclicPathTest.StandardTestImpl.class,
            GroovyDedupTest.StandardTestImpl.class,
            GroovyExceptTest.StandardTestImpl.class,
            GroovyFilterTest.StandardTestImpl.class,
            GroovyHasNotTest.StandardTestImpl.class,
            GroovyHasTest.StandardTestImpl.class,
            GroovyIntervalTest.StandardTestImpl.class,
            GroovyRandomTest.StandardTestImpl.class,
            GroovyRangeTest.StandardTestImpl.class,
            GroovyRetainTest.StandardTestImpl.class,
            GroovySimplePathTest.StandardTestImpl.class,
            GroovyWhereTest.StandardTestImpl.class,
            // map
            GroovyBackTest.StandardTestImpl.class,
            GroovyChooseTest.StandardTestImpl.class,
            GroovyFoldTest.StandardTestImpl.class,
            GroovyHiddenValueMapTest.StandardTestImpl.class,
            GroovyJumpTest.StandardTestImpl.class,
            GroovyMapTest.StandardTestImpl.class,
            GroovyMatchTest.StandardTestImpl.class,
            GroovyOrderByTest.StandardTestImpl.class,
            GroovyOrderTest.StandardTestImpl.class,
            GroovyPathTest.StandardTestImpl.class,
            GroovySelectTest.StandardTestImpl.class,
            GroovyUnfoldTest.StandardTestImpl.class,
            GroovyUntilTest.StandardTestImpl.class,
            GroovyValueMapTest.StandardTestImpl.class,
            GroovyVertexTest.StandardTestImpl.class,
            // sideEffect
            GroovyAggregateTest.StandardTestImpl.class,
            GroovyCountTest.StandardTestImpl.class,
            GroovyGroupByTest.StandardTestImpl.class,
            GroovyGroupCountTest.StandardTestImpl.class,
            GroovyInjectTest.StandardTestImpl.class,
            GroovySideEffectCapTest.StandardTestImpl.class,
            GroovyStoreTest.StandardTestImpl.class,
            GroovyTreeTest.StandardTestImpl.class,

            // util
            TraversalSideEffectsTest.StandardTestImpl.class,

            // compliance
            TraversalCoverageTestImpl.class,
            CoreTraversalTestImpl.class,
    };


    public GroovyProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce, true);
    }
}
