package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.groovy.loaders.GremlinLoader;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.process.computer.GroovyGraphComputerTest;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyCyclicPathTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyFilterTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyHasNotTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyHasTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyIntervalTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyRandomTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovySimplePathTestImpl;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyWhereTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyBackTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyHiddenValueMapTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyJumpTestImpl;
import com.tinkerpop.gremlin.process.graph.step.map.GroovyMapTestImpl;
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
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyProcessComputerSuite extends ProcessComputerSuite {
    static {
        GremlinLoader.load();
        SugarLoader.load();
    }

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            GroovyGraphComputerTest.ComputerTestImpl.class,
            // filter
            GroovyCyclicPathTestImpl.ComputerTestImpl.class,
            // TODO: GroovyDedupTest.ComputerTest.class
            // TODO: GroovyExceptTest.ComputerTest.class,
            GroovyFilterTestImpl.ComputerTestImpl.class,
            GroovyHasNotTestImpl.ComputerTestImpl.class,
            GroovyHasTestImpl.ComputerTestImpl.class,
            GroovyIntervalTestImpl.ComputerTestImpl.class,
            GroovyRandomTestImpl.ComputerTestImpl.class,
            // TODO: GroovyRangeTest.ComputerTest.class,
            // TODO: GroovyRetainTest.ComputerTest.class,
            GroovySimplePathTestImpl.ComputerTestImpl.class,
            GroovyWhereTestImpl.ComputerTestImpl.class,

            // map
            GroovyBackTestImpl.ComputerTestImpl.class,
            // TODO: GroovyChooseTest.ComputerTest.class,
            // TODO: GroovyFoldTest.ComputerTest.class,
            GroovyHiddenValueMapTestImpl.ComputerTestImpl.class,
            GroovyJumpTestImpl.ComputerTestImpl.class,
            GroovyMapTestImpl.ComputerTestImpl.class,
            // TODO: GroovyMatchTest.ComputerTest.class,
            // TODO: GroovyOrderByTest.ComputerTest.class,
            // TODO: GroovyOrderTest.ComputerTest.class,
            GroovyPathTestImpl.ComputerTestImpl.class,
            GroovySelectTestImpl.ComputerTestImpl.class,
            GroovyUnfoldTestImpl.ComputerTestImpl.class,
            // TODO: GroovyUnionTest.ComputerTest.class,
            GroovyUntilTestImpl.ComputerTestImpl.class,
            GroovyValueMapTestImpl.ComputerTestImpl.class,
            GroovyVertexTestImpl.ComputerTestImpl.class,

            // sideEffect
            // TODO: GroovyAddEdgeTest.ComputerTest.class,
            GroovyAggregateTestImpl.ComputerTestImpl.class,
            GroovyCountTestImpl.ComputerTestImpl.class,
            GroovyGroupByTestImpl.ComputerTestImpl.class,
            GroovyGroupCountTestImpl.ComputerTestImpl.class,
            GroovyInjectTestImpl.ComputerTestImpl.class,
            GroovySideEffectCapTestImpl.ComputerTestImpl.class,
            GroovyStoreTestImpl.ComputerTestImpl.class,
            GroovyTreeTestImpl.ComputerTestImpl.class
    };


    public GroovyProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }
}
