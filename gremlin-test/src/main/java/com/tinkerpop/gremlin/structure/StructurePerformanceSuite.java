package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The {@code StructurePerformanceSuite} is a JUnit test runner that executes the Gremlin Test Suite over a
 * {@link com.tinkerpop.gremlin.structure.Graph} implementation.  This specialized test suite and runner is for use
 * by Gremlin Structure implementers to test their {@link com.tinkerpop.gremlin.structure.Graph}implementations.
 * The {@code StructurePerformanceSuite} runs more complex testing scenarios over
 * {@link com.tinkerpop.gremlin.structure.Graph} implementations than the standard {@code StructurePerformanceSuite}.
 * <p/>
 * To use the {@code StructurePerformanceSuite} define a class in a test module.  Simple naming would expect the name
 * of the implementation followed by "StructurePerformanceTest".  This class should be annotated as follows (note that
 * the "Suite" implements {@link com.tinkerpop.gremlin.GraphProvider} as a convenience only. It could be implemented
 * in a separate class file):
 * <code>
 * @RunWith(StructurePerformanceSuite.class)
 * @StructurePerformanceSuite.GraphProviderClass(TinkerGraphStructurePerformanceTest.class) public class TinkerGraphStructurePerformanceTest implements GraphProvider {
 * }
 * </code>
 * Implementing {@link com.tinkerpop.gremlin.GraphProvider} provides a way for the {@code StructurePerformanceSuite}
 * to instantiate {@link com.tinkerpop.gremlin.structure.Graph} instances from the implementation being tested to
 * inject into tests in the suite.  The {@code StructurePerformanceSuite} will utilized
 * {@link com.tinkerpop.gremlin.structure.Graph.Features} defined in the suite to determine which tests will be
 * executed.
 * <br/>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StructurePerformanceSuite extends AbstractGremlinSuite {
    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            GraphWritePerformanceTest.class,
            GraphReadPerformanceTest.class
    };

    public StructurePerformanceSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute);
    }


}
