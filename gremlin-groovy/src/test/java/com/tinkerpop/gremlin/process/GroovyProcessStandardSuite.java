package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.groovy.loaders.GremlinLoader;
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
import com.tinkerpop.gremlin.process.graph.step.map.GroovyBackTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyChooseTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyJumpTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyMapTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyMatchTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyOrderTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyPathTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovySelectTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyTraversalTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyValuesTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyAggregateTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupByTestImpl;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroovyGroupCountTestImpl;
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
    }

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            // filter
            GroovyCyclicPathTestImpl.class,
            GroovyDedupTestImpl.class,
            GroovyExceptTestImpl.class,
            GroovyFilterTestImpl.class,
            GroovyHasNotTestImpl.class,
            GroovyHasTestImpl.class,
            GroovyIntervalTestImpl.class,
            GroovyRandomTestImpl.class,
            GroovyRangeTestImpl.class,
            GroovyRetainTestImpl.class,
            GroovySimplePathTestImpl.class,
            // map
            GroovyBackTestImpl.class,
            GroovyChooseTestImpl.class,
            GroovyJumpTestImpl.class,
            GroovyMapTestImpl.class,
            GroovyMatchTestImpl.class,
            GroovyOrderTestImpl.class,
            GroovyPathTestImpl.class,
            GroovySelectTestImpl.class,
            GroovyTraversalTestImpl.class,
            GroovyValuesTestImpl.class,
            // sideEffect
            GroovyAggregateTestImpl.class,
            GroovyGroupByTestImpl.class,
            GroovyGroupCountTestImpl.class,
            GroovyTreeTestImpl.class
    };


    public GroovyProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }
}
