package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest;
import com.tinkerpop.gremlin.algorithm.generator.DistributionGeneratorTest;
import com.tinkerpop.gremlin.structure.strategy.IdGraphStrategyTest;
import com.tinkerpop.gremlin.structure.strategy.PartitionGraphStrategyTest;
import com.tinkerpop.gremlin.structure.strategy.ReadOnlyGraphStrategyTest;
import com.tinkerpop.gremlin.structure.strategy.SequenceGraphStrategyTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The StructureStandardSuite is a custom JUnit test runner that executes the Gremlin Test Suite over a Graph
 * implementation.  This specialized test suite and runner is for use by Blueprints implementers to test their
 * Graph implementations.  The ProcessStandardSuite ensures consistency and validity of the implementations that they
 * test.
 * <p/>
 * To use the StructureStandardSuite define a class in a test module.  Simple naming would expect the name of the
 * implementation followed by "StructureStandardSuite".  This class should be annotated as follows (note that the "Suite"
 * implements StructureStandardSuite.GraphProvider as a convenience only...it could be implemented in a separate class
 * file):
 * <code>
 *
 * @RunWith(StructureStandardSuite.class)
 * @BlueprintsSuite.GraphProviderClass(MsAccessBlueprintsTest.class)
 * public class MsAccessBlueprintsTest implements GraphProvider {
 * }
 * </code>
 * Implementing {@link com.tinkerpop.gremlin.GraphProvider} provides a way for the StructureStandardSuite to
 * instantiate Graph instances from the implementation being tested to inject into tests in the suite.  The
 * StructureStandardSuite will utilized Features defined in the suite to determine which tests will be executed.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StructureStandardSuite extends AbstractGremlinSuite {

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            AnnotationTest.class,
            CommunityGeneratorTest.class,
            DistributionGeneratorTest.class,
            EdgeTest.class,
            ExceptionConsistencyTest.class,
            FeatureSupportTest.class,
            GraphTest.class,
            GraphComputerTest.class,
            GraphConstructionTest.class,
            IdGraphStrategyTest.class,
            IoTest.class,
            MemoryTest.class,
            PartitionGraphStrategyTest.class,
            PropertyTest.class,
            ReadOnlyGraphStrategyTest.class,
            SequenceGraphStrategyTest.class,
            VertexTest.class
    };


    public StructureStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute);
    }
}
