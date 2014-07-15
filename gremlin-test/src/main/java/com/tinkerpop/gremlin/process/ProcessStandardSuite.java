package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.process.graph.step.filter.CyclicPathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.DedupTest;
import com.tinkerpop.gremlin.process.graph.step.filter.ExceptTest;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterTest;
import com.tinkerpop.gremlin.process.graph.step.filter.HasTest;
import com.tinkerpop.gremlin.process.graph.step.filter.IntervalTest;
import com.tinkerpop.gremlin.process.graph.step.filter.RandomTest;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeTest;
import com.tinkerpop.gremlin.process.graph.step.filter.RetainTest;
import com.tinkerpop.gremlin.process.graph.step.filter.SimplePathTest;
import com.tinkerpop.gremlin.process.graph.step.map.BackTest;
import com.tinkerpop.gremlin.process.graph.step.map.FoldTest;
import com.tinkerpop.gremlin.process.graph.step.map.JumpTest;
import com.tinkerpop.gremlin.process.graph.step.map.MapTest;
import com.tinkerpop.gremlin.process.graph.step.map.OrderTest;
import com.tinkerpop.gremlin.process.graph.step.map.PathTest;
import com.tinkerpop.gremlin.process.graph.step.map.SelectTest;
import com.tinkerpop.gremlin.process.graph.step.map.TraversalTest;
import com.tinkerpop.gremlin.process.graph.step.map.UnfoldTest;
import com.tinkerpop.gremlin.process.graph.step.map.ValuesTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AddEdgeTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SubgraphTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.TreeTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The ProcessStandardSuite is a custom JUnit test runner that executes the Gremlin Test Suite over a Graph
 * implementation.  This specialized test suite and runner is for use by Blueprints implementers to test their
 * Graph implementations.  The StructureStandardSuite ensures consistency and validity of the implementations that they
 * test.
 * <p>
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
 * Implementing {@link com.tinkerpop.gremlin.GraphProvider} provides a way for the ProcessStandardSuite to
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
            RandomTest.JavaRandomTest.class,
            RangeTest.JavaRangeTest.class,
            RetainTest.JavaRetainTest.class,
            SimplePathTest.JavaSimplePathTest.class,
            BackTest.JavaBackTest.class,
            JumpTest.JavaJumpTest.class,
            MapTest.JavaMapTest.class,
            OrderTest.JavaOrderTest.class,
            PathTest.JavaPathTest.class,
            SelectTest.JavaSelectTest.class,
            TraversalTest.JavaTraversalTest.class,
            ValuesTest.JavaValuesTest.class,
            UnfoldTest.JavaUnfoldTest.class,
            FoldTest.JavaFoldTest.class,
            AggregateTest.JavaAggregateTest.class,
            CountTest.JavaCountTest.class,
            GroupByTest.JavaGroupByTest.class,
            GroupCountTest.JavaGroupCountTest.class,
            AddEdgeTest.JavaAddEdgeTest.class,
            SideEffectTest.JavaSideEffectTest.class,
            SideEffectCapTest.JavaSideEffectCapTest.class,
            SubgraphTest.JavaSideEffectTest.class,
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
            RandomTest.class,
            RangeTest.class,
            RetainTest.class,
            SimplePathTest.class,
            BackTest.class,
            JumpTest.class,
            MapTest.class,
            OrderTest.class,
            PathTest.class,
            SelectTest.class,
            TraversalTest.class,
            ValuesTest.class,
            UnfoldTest.class,
            FoldTest.class,
            AggregateTest.class,
            CountTest.class,
            GroupByTest.class,
            GroupCountTest.class,
            AddEdgeTest.class,
            SideEffectTest.class,
            SideEffectCapTest.class,
            SubgraphTest.class,
            TreeTest.class
    };

    public ProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }

    public ProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }
}
