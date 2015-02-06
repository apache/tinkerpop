package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.structure.GraphReadPerformanceTest;
import com.tinkerpop.gremlin.structure.GraphWritePerformanceTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The {@code ProcessPerformanceSuite} is a JUnit test runner that executes the Gremlin Test Suite over a Graph
 * implementation.  This specialized test suite and runner is for use by Gremlin implementers to test their
 * Graph implementations traversal speeds.
 * <p/>
 * To use the {@code ProcessPerformanceSuite} define a class in a test module.  Simple naming would expect the name of
 * the implementation followed by "ProcessPerformanceTest".  This class should be annotated as follows (note that
 * the "Suite" implements {@link com.tinkerpop.gremlin.GraphProvider} as a convenience only. It could be implemented in
 * a separate class file):
 * <code>
 * @RunWith(ProcessPerformanceSuite.class)
 * @SProcessPerformanceSuite.GraphProviderClass(TinkerGraphProcessPerformanceTest.class)
 * public class TinkerGraphProcessPerformanceTest implements GraphProvider {
 * }
 * </code>
 * Implementing {@link com.tinkerpop.gremlin.GraphProvider} provides a way for the {@code ProcessPerformanceSuite} to
 * instantiate {@link com.tinkerpop.gremlin.structure.Graph} instances from the implementation being tested to inject
 * into tests in the suite.  The {@code ProcessPerformanceSuite} will utilized
 * {@link com.tinkerpop.gremlin.structure.Graph.Features} defined in the suite to determine which tests will be
 * executed.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProcessPerformanceSuite extends AbstractGremlinSuite {
    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            TraversalPerformanceTest.class
    };

    public ProcessPerformanceSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute);
    }


}
