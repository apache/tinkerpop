package com.tinkerpop.blueprints;

import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The BlueprintsPerformanceSuite is a custom JUnit test runner that executes the Blueprints Test Suite over a Graph
 * implementation.  This specialized test suite and runner is for use by Blueprints implementers to test their
 * Graph implementations.  The BlueprintsPerformanceSuite runs more complex testing scenarios over Graph
 * implementations than the standard BlueprintsStandardSuite.
 * <p/>
 * To use the BlueprintsPerformanceSuite define a class in a test module.  Simple naming would expect the name of the
 * implementation followed by "BlueprintsPerformanceTest".  This class should be annotated as follows (note that
 * the "Suite" implements BlueprintsStandardSuite.GraphProvider as a convenience only...it could be implemented in a separate
 * class file):
 * <code>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @RunWith(BlueprintsPerformanceSuite.class)
 * @BlueprintsSuite.GraphProviderClass(MsAccessBlueprintsPerformanceTest.class)
 * public class MsAccessBlueprintsTest implements AbstractBlueprintsSuite.GraphProvider {
 * }
 * </code>
 * Implementing AbstractBlueprintsSuite.GraphProvider provides a way for the BlueprintsPerformanceSuite to instantiate
 * Graph instances from the implementation being tested to inject into tests in the suite.  The
 * BlueprintsPerformanceSuite will utilized Features defined in the suite to determine which tests will be executed.
 */

public class BlueprintsPerformanceSuite extends AbstractBlueprintsSuite {
    /**
     * This list of tests in the suite that will be executed.  Blueprints developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            GraphGeneratePerformanceTest.class,
            GraphReadPerformanceTest.class
    };

    public BlueprintsPerformanceSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute);
    }


}
