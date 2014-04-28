package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.groovy.GremlinLoader;
import com.tinkerpop.gremlin.process.graph.filter.GroovyCyclicPathTest;
import com.tinkerpop.gremlin.process.graph.filter.GroovyDedupTest;
import com.tinkerpop.gremlin.process.graph.filter.GroovyExceptTest;
import com.tinkerpop.gremlin.process.graph.filter.GroovyFilterTest;
import com.tinkerpop.gremlin.process.graph.filter.GroovyHasTest;
import com.tinkerpop.gremlin.process.graph.filter.GroovyIntervalTest;
import com.tinkerpop.gremlin.process.graph.filter.GroovyRandomTest;
import com.tinkerpop.gremlin.process.graph.filter.GroovyRangeTest;
import com.tinkerpop.gremlin.process.graph.filter.GroovyRetainTest;
import com.tinkerpop.gremlin.process.graph.filter.GroovySimplePathTest;
import com.tinkerpop.gremlin.process.graph.map.GroovyAnnotatedValuesTest;
import com.tinkerpop.gremlin.process.graph.map.GroovyBackTest;
import com.tinkerpop.gremlin.process.graph.map.GroovyJumpTest;
import com.tinkerpop.gremlin.process.graph.map.GroovyOrderTest;
import com.tinkerpop.gremlin.process.graph.map.GroovyPathTest;
import com.tinkerpop.gremlin.process.graph.map.GroovySelectTest;
import com.tinkerpop.gremlin.process.graph.map.GroovyTraversalTest;
import com.tinkerpop.gremlin.process.graph.map.GroovyValuesTest;
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
            GroovyCyclicPathTest.class,
            GroovyDedupTest.class,
            GroovyExceptTest.class,
            GroovyFilterTest.class,
            GroovyHasTest.class,
            GroovyIntervalTest.class,
            GroovyRandomTest.class,
            GroovyRangeTest.class,
            GroovyRetainTest.class,
            GroovySimplePathTest.class,
            GroovyAnnotatedValuesTest.class,
            GroovyBackTest.class,
            GroovyJumpTest.class,
            GroovyOrderTest.class,
            GroovyPathTest.class,
            GroovySelectTest.class,
            GroovyTraversalTest.class,
            GroovyValuesTest.class
    };


    public GroovyProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }
}
