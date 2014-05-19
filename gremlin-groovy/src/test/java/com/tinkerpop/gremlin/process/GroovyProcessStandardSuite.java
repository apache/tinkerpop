package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.groovy.GremlinLoader;
import com.tinkerpop.gremlin.process.graph.filter.GroovyCyclicPathTestG;
import com.tinkerpop.gremlin.process.graph.filter.GroovyDedupTestG;
import com.tinkerpop.gremlin.process.graph.filter.GroovyExceptTestG;
import com.tinkerpop.gremlin.process.graph.filter.GroovyFilterTestG;
import com.tinkerpop.gremlin.process.graph.filter.GroovyHasTestG;
import com.tinkerpop.gremlin.process.graph.filter.GroovyIntervalTestG;
import com.tinkerpop.gremlin.process.graph.filter.GroovyRandomTestG;
import com.tinkerpop.gremlin.process.graph.filter.GroovyRangeTestG;
import com.tinkerpop.gremlin.process.graph.filter.GroovyRetainTestG;
import com.tinkerpop.gremlin.process.graph.filter.GroovySimplePathTestG;
import com.tinkerpop.gremlin.process.graph.map.GroovyBackTestG;
import com.tinkerpop.gremlin.process.graph.map.GroovyJumpTestG;
import com.tinkerpop.gremlin.process.graph.map.GroovyOrderTestG;
import com.tinkerpop.gremlin.process.graph.map.GroovyPathTestG;
import com.tinkerpop.gremlin.process.graph.map.GroovySelectTestG;
import com.tinkerpop.gremlin.process.graph.map.GroovyTraversalTestG;
import com.tinkerpop.gremlin.process.graph.map.GroovyValuesTestG;
import com.tinkerpop.gremlin.process.graph.sideEffect.GroovyAggregateTestG;
import com.tinkerpop.gremlin.process.graph.sideEffect.GroovyGroupByTestG;
import com.tinkerpop.gremlin.process.graph.sideEffect.GroovyGroupCountTestG;
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
            GroovyCyclicPathTestG.class,
            GroovyDedupTestG.class,
            GroovyExceptTestG.class,
            GroovyFilterTestG.class,
            GroovyHasTestG.class,
            GroovyIntervalTestG.class,
            GroovyRandomTestG.class,
            GroovyRangeTestG.class,
            GroovyRetainTestG.class,
            GroovySimplePathTestG.class,
            GroovyBackTestG.class,
            GroovyJumpTestG.class,
            GroovyOrderTestG.class,
            GroovyPathTestG.class,
            GroovySelectTestG.class,
            GroovyTraversalTestG.class,
            GroovyValuesTestG.class,
            GroovyAggregateTestG.class,
            GroovyGroupByTestG.class,
            GroovyGroupCountTestG.class
    };


    public GroovyProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }
}
