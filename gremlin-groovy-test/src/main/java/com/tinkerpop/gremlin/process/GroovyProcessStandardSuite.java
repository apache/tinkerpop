package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.groovy.loaders.GremlinLoader;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyCyclicPathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyDedupTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyExceptTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyFilterTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyHasNotTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyHasTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyIntervalTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyRandomTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyRangeTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyRetainTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovySimplePathTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyWhereTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyBackTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyChooseTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyFoldTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyJumpTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyMapTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyMatchTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyOrderByTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyOrderTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyPathTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovySelectTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyUnfoldTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyValueMapTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyVertexTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyAggregateTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupByTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupCountTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyStoreTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyTreeTestImpl;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The test suite for the Groovy implementation of Gremlin Process.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyProcessStandardSuite extends ProcessStandardSuite {

    static {
        GremlinLoader.load();
        SugarLoader.load();
    }

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            // filter
            GroovyCyclicPathTest.StandardTest.class,
            GroovyDedupTest.StandardTest.class,
            GroovyExceptTest.StandardTest.class,
            GroovyFilterTest.StandardTest.class,
            GroovyHasNotTest.StandardTest.class,
            GroovyHasTest.StandardTest.class,
            GroovyIntervalTest.StandardTest.class,
            GroovyRandomTest.StandardTest.class,
            GroovyRangeTestImpl.class,
            GroovyRetainTestImpl.class,
            GroovySimplePathTestImpl.class,
            GroovyWhereTestImpl.class,
            // map
            GroovyBackTestImpl.class,
            GroovyChooseTestImpl.class,
            GroovyFoldTestImpl.class,
            GroovyJumpTestImpl.class,
            GroovyMapTestImpl.class,
            GroovyMatchTestImpl.class,
            GroovyOrderTestImpl.class,
            GroovyOrderByTestImpl.class,
            GroovyPathTestImpl.class,
            GroovySelectTestImpl.class,
            GroovyUnfoldTestImpl.class,
            GroovyValueMapTestImpl.class,
            GroovyVertexTestImpl.class,
            // sideEffect
            GroovyAggregateTestImpl.class,
            GroovyGroupByTestImpl.class,
            GroovyGroupCountTestImpl.class,
            GroovyStoreTestImpl.class,
            GroovyTreeTestImpl.class
    };


    public GroovyProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce, true);
    }
}
