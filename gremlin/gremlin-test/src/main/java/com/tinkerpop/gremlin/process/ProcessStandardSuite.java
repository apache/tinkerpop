package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.process.steps.filter.DedupTest;
import com.tinkerpop.gremlin.process.steps.filter.ExceptTest;
import com.tinkerpop.gremlin.process.steps.filter.FilterTest;
import com.tinkerpop.gremlin.process.steps.filter.HasTest;
import com.tinkerpop.gremlin.process.steps.filter.IntervalTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The ProcessStandardSuite is a custom JUnit test runner that executes the Gremlin Test Suite over a Graph
 * implementation.  This specialized test suite and runner is for use by Blueprints implementers to test their
 * Graph implementations.  The StructureStandardSuite ensures consistency and validity of the implementations that they
 * test.
 * <p/>
 * To use the ProcessStandardSuite define a class in a test module.  Simple naming would expect the name of the
 * implementation followed by "ProcessStandardSuite".  This class should be annotated as follows (note that the "Suite"
 * implements ProcessStandardSuite.GraphProvider as a convenience only...it could be implemented in a separate class file):
 * <code>
 *
 * @RunWith(ProcessStandardSuite.class)
 * @BlueprintsSuite.GraphProviderClass(MsAccessBlueprintsTest.class)
 * public class MsAccessBlueprintsTest implements GraphProvider {
 * }
 * </code>
 * Implementing {@link com.tinkerpop.gremlin.GraphProvider} provides a way for the StructureStandardSuite to
 * instantiate Graph instances from the implementation being tested to inject into tests in the suite.  The
 * ProcessStandardSuite will utilized Features defined in the suite to determine which tests will be executed.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProcessStandardSuite extends AbstractGremlinSuite {

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            DedupTest.JavaDedupTest.class,
            ExceptTest.JavaExceptTest.class,
            FilterTest.JavaFilterTest.class,
            HasTest.JavaHasTest.class,
            IntervalTest.JavaIntervalTest.class
    };


    public ProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute);
    }
}
