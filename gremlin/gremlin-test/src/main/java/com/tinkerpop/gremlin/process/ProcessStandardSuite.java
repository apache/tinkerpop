package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.process.graph.filter.CyclicPathTest;
import com.tinkerpop.gremlin.process.graph.filter.DedupTest;
import com.tinkerpop.gremlin.process.graph.filter.ExceptTest;
import com.tinkerpop.gremlin.process.graph.filter.FilterTest;
import com.tinkerpop.gremlin.process.graph.filter.HasTest;
import com.tinkerpop.gremlin.process.graph.filter.IntervalTest;
import com.tinkerpop.gremlin.process.graph.filter.RangeTest;
import com.tinkerpop.gremlin.process.graph.filter.RetainTest;
import com.tinkerpop.gremlin.process.graph.filter.SimplePathTest;
import com.tinkerpop.gremlin.process.graph.map.AnnotatedValuesTest;
import com.tinkerpop.gremlin.process.graph.map.BackTest;
import com.tinkerpop.gremlin.process.graph.map.JumpTest;
import com.tinkerpop.gremlin.process.graph.map.OrderTest;
import com.tinkerpop.gremlin.process.graph.map.PathTest;
import com.tinkerpop.gremlin.process.graph.map.SelectTest;
import com.tinkerpop.gremlin.process.graph.map.TraversalTest;
import com.tinkerpop.gremlin.process.graph.map.ValuesTest;
import com.tinkerpop.gremlin.process.graph.sideEffect.AggregateTest;
import com.tinkerpop.gremlin.process.graph.sideEffect.GroupByTest;
import com.tinkerpop.gremlin.process.graph.sideEffect.GroupCountTest;
import com.tinkerpop.gremlin.process.graph.sideEffect.LinkTest;
import com.tinkerpop.gremlin.process.graph.sideEffect.SideEffectTest;
import com.tinkerpop.gremlin.process.graph.sideEffect.TreeTest;
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
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @RunWith(ProcessStandardSuite.class)
 * @BlueprintsSuite.GraphProviderClass(MsAccessBlueprintsTest.class) public class MsAccessBlueprintsTest implements GraphProvider {
 * }
 * </code>
 * Implementing {@link com.tinkerpop.gremlin.GraphProvider} provides a way for the StructureStandardSuite to
 * instantiate Graph instances from the implementation being tested to inject into tests in the suite.  The
 * ProcessStandardSuite will utilized Features defined in the suite to determine which tests will be executed.
 */
public class ProcessStandardSuite extends AbstractGremlinSuite {

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            CyclicPathTest.JavaCyclicPathTest.class,
            DedupTest.JavaDedupTest.class,
            ExceptTest.JavaExceptTest.class,
            FilterTest.JavaFilterTest.class,
            HasTest.JavaHasTest.class,
            IntervalTest.JavaIntervalTest.class,
            RangeTest.JavaRangeTest.class,
            RetainTest.JavaRetainTest.class,
            SimplePathTest.JavaSimplePathTest.class,
            AnnotatedValuesTest.JavaAnnotatedValuesTest.class,
            BackTest.JavaBackTest.class,
            JumpTest.JavaJumpTest.class,
            OrderTest.JavaOrderTest.class,
            PathTest.JavaPathTest.class,
            SelectTest.JavaSelectTest.class,
            TraversalTest.JavaTraversalTest.class,
            ValuesTest.JavaValuesTest.class,
            AggregateTest.JavaAggregateTest.class,
            GroupByTest.JavaGroupByTest.class,
            GroupCountTest.JavaGroupCountTest.class,
            LinkTest.JavaLinkTest.class,
            SideEffectTest.JavaSideEffectTest.class,
            TreeTest.JavaTreeTest.class
    };

    /**
     * Tests that will be enforced by the suite where instances of them should be in the list of testsToExecute.
     */
    protected static final Class<?>[] testsToEnforce = new Class<?>[]{
            DedupTest.class,
            ExceptTest.class,
            FilterTest.class,
            HasTest.class,
            IntervalTest.class,
            RangeTest.class,
            RetainTest.class,
            AnnotatedValuesTest.class,
            BackTest.class,
            JumpTest.class,
            OrderTest.class,
            PathTest.class,
            SelectTest.class,
            TraversalTest.class,
            ValuesTest.class,
            AggregateTest.class,
            GroupByTest.class,
            GroupCountTest.class,
            LinkTest.class,
            SideEffectTest.class,
            TreeTest.class
    };

    public ProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }

    public ProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }
}
