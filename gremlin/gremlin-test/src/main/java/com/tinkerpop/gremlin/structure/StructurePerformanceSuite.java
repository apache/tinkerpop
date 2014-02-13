package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The StructurePerformanceSuite is a custom JUnit test runner that executes the Gremlin Test Suite over a Graph
 * implementation.  This specialized test suite and runner is for use by Gremlin Structure implementers to test their
 * Graph implementations.  The StructurePerformanceSuite runs more complex testing scenarios over Graph
 * implementations than the standard StructureStandardSuite.
 * <p/>
 * To use the StructurePerformanceSuite define a class in a test module.  Simple naming would expect the name of the
 * implementation followed by "StructurePerformanceTest".  This class should be annotated as follows (note that
 * the "Suite" implements StructureStandardSuite.GraphProvider as a convenience only...it could be implemented in a separate
 * class file):
 * <code>
 *
 * @RunWith(BlueprintsPerformanceSuite.class)
 * @StructurePerformanceSuite.GraphProviderClass(MsAccessBlueprintsPerformanceTest.class)
 * public class MsAccessStructurePerformanceTest implements AbstractGremlinSuite.GraphProvider {
 * }
 * </code>
 * Implementing AbstractGremlinSuite.GraphProvider provides a way for the StructurePerformanceSuite to instantiate
 * Graph instances from the implementation being tested to inject into tests in the suite.  The
 * StructurePerformanceSuite will utilized Features defined in the suite to determine which tests will be executed.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */

public class StructurePerformanceSuite extends AbstractGremlinSuite {
    /**
     * This list of tests in the suite that will be executed.  Blueprints developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            GraphGeneratePerformanceTest.class,
            GraphReadPerformanceTest.class
    };

    public StructurePerformanceSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute);
    }


}
