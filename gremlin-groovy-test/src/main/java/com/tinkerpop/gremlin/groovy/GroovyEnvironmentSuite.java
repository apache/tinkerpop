package com.tinkerpop.gremlin.groovy;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.groovy.engine.GremlinExecutorTest;
import com.tinkerpop.gremlin.groovy.engine.GroovyTraversalScriptTest;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineTest;
import com.tinkerpop.gremlin.groovy.loaders.GremlinLoader;
import com.tinkerpop.gremlin.groovy.loaders.GremlinLoaderTest;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoaderTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyEnvironmentSuite extends AbstractGremlinSuite {

    static {
        GremlinLoader.load();
    }

    private static final Class<?>[] allTests = new Class<?>[]{
            GremlinGroovyScriptEngineTest.class,
            GremlinExecutorTest.class,
            GremlinLoaderTest.class,
            GroovyTraversalScriptTest.class
            //SugarLoaderTest.class,
    };

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute;

    static {
        final String override = System.getenv().getOrDefault("gremlin.tests", "");
        if (override.equals(""))
            testsToExecute = allTests;
        else {
            final List<String> filters = Arrays.asList(override.split(","));
            final List<Class<?>> allowed = Stream.of(allTests)
                    .filter(c -> filters.contains(c.getName()))
                    .collect(Collectors.toList());
            testsToExecute = allowed.toArray(new Class<?>[allowed.size()]);
        }
    }

    public GroovyEnvironmentSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute);
    }
}
