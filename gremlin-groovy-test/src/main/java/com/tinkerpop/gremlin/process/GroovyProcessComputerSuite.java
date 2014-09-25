package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.groovy.loaders.GremlinLoader;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.process.graph.step.filter.GroovyCyclicPathTestImpl;
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
            GroovyCyclicPathTestImpl.ComputerTestImpl.class,
    };


    public GroovyProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }
}
