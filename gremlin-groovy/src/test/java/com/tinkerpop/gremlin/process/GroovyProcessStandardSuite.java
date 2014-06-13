package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.groovy.GremlinLoader;
import com.tinkerpop.gremlin.process.graph.filter.GroovyCyclicPathTestImpl;
import com.tinkerpop.gremlin.process.graph.filter.GroovyDedupTestImpl;
import com.tinkerpop.gremlin.process.graph.filter.GroovyExceptTestImpl;
import com.tinkerpop.gremlin.process.graph.filter.GroovyFilterTestImpl;
import com.tinkerpop.gremlin.process.graph.filter.GroovyHasTestImpl;
import com.tinkerpop.gremlin.process.graph.filter.GroovyIntervalTestImpl;
import com.tinkerpop.gremlin.process.graph.filter.GroovyRandomTestImpl;
import com.tinkerpop.gremlin.process.graph.filter.GroovyRangeTestImpl;
import com.tinkerpop.gremlin.process.graph.filter.GroovyRetainTestImpl;
import com.tinkerpop.gremlin.process.graph.filter.GroovySimplePathTestImpl;
import com.tinkerpop.gremlin.process.graph.map.GroovyBackTestImpl;
import com.tinkerpop.gremlin.process.graph.map.GroovyJumpTestImpl;
import com.tinkerpop.gremlin.process.graph.map.GroovyMapTestImpl;
import com.tinkerpop.gremlin.process.graph.map.GroovyOrderTestImpl;
import com.tinkerpop.gremlin.process.graph.map.GroovyPathTestImpl;
import com.tinkerpop.gremlin.process.graph.map.GroovySelectTestImpl;
import com.tinkerpop.gremlin.process.graph.map.GroovyTraversalTestImpl;
import com.tinkerpop.gremlin.process.graph.map.GroovyValuesTestImpl;
import com.tinkerpop.gremlin.process.graph.sideEffect.GroovyAggregateTestImpl;
import com.tinkerpop.gremlin.process.graph.sideEffect.GroovyGroupByTestImpl;
import com.tinkerpop.gremlin.process.graph.sideEffect.GroovyGroupCountTestImpl;
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
            GroovyCyclicPathTestImpl.class,
            GroovyDedupTestImpl.class,
            GroovyExceptTestImpl.class,
            GroovyFilterTestImpl.class,
            GroovyHasTestImpl.class,
            GroovyIntervalTestImpl.class,
            GroovyRandomTestImpl.class,
            GroovyRangeTestImpl.class,
            GroovyRetainTestImpl.class,
            GroovySimplePathTestImpl.class,
            GroovyBackTestImpl.class,
            GroovyJumpTestImpl.class,
			GroovyMapTestImpl.class,
            GroovyOrderTestImpl.class,
            GroovyPathTestImpl.class,
            GroovySelectTestImpl.class,
            GroovyTraversalTestImpl.class,
            GroovyValuesTestImpl.class,
            GroovyAggregateTestImpl.class,
            GroovyGroupByTestImpl.class,
            GroovyGroupCountTestImpl.class
    };


    public GroovyProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }
}
