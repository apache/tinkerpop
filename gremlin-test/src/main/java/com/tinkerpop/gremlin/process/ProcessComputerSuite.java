package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgramTest;
import com.tinkerpop.gremlin.process.graph.step.filter.CyclicPathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.ExceptTest;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterTest;
import com.tinkerpop.gremlin.process.graph.step.filter.HasNotTest;
import com.tinkerpop.gremlin.process.graph.step.filter.HasTest;
import com.tinkerpop.gremlin.process.graph.step.filter.IntervalTest;
import com.tinkerpop.gremlin.process.graph.step.filter.RandomTest;
import com.tinkerpop.gremlin.process.graph.step.filter.RetainTest;
import com.tinkerpop.gremlin.process.graph.step.filter.SimplePathTest;
import com.tinkerpop.gremlin.process.graph.step.map.BackTest;
import com.tinkerpop.gremlin.process.graph.step.map.JumpTest;
import com.tinkerpop.gremlin.process.graph.step.map.MapTest;
import com.tinkerpop.gremlin.process.graph.step.map.PathTest;
import com.tinkerpop.gremlin.process.graph.step.map.SelectTest;
import com.tinkerpop.gremlin.process.graph.step.map.TraversalTest;
import com.tinkerpop.gremlin.process.graph.step.map.UnfoldTest;
import com.tinkerpop.gremlin.process.graph.step.map.UnionTest;
import com.tinkerpop.gremlin.process.graph.step.map.ValuesTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.TreeTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The ProcessComputerStandardSuite is a custom JUnit test runner that executes the Gremlin Test Suite over a Graph
 * implementation.  This specialized test suite and runner is for use by Blueprints implementers to test their
 * Graph implementations.  The StructureStandardSuite ensures consistency and validity of the implementations that they
 * test.
 * <p>
 * To use the ProcessComputerStandardSuite define a class in a test module.  Simple naming would expect the name of the
 * implementation followed by "ProcessComputerStandardSuite".  This class should be annotated as follows (note that the "Suite"
 * implements ProcessComputerStandardSuite.GraphProvider as a convenience only...it could be implemented in a separate class file):
 * <code>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @RunWith(ProcessComputerStandardSuite.class)
 * @BlueprintsSuite.GraphProviderClass(MsAccessBlueprintsTest.class) public class MsAccessBlueprintsTest implements GraphProvider {
 * }
 * </code>
 * Implementing {@link com.tinkerpop.gremlin.GraphProvider} provides a way for the ProcessComputerStandardSuite to
 * instantiate Graph instances from the implementation being tested to inject into tests in the suite.  The
 * ProcessStandardSuite will utilized Features defined in the suite to determine which tests will be executed.
 */
public class ProcessComputerSuite extends AbstractGremlinSuite {

    // todo: all tests are not currently passing. see specific todos in each test

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{
            // filter
            CyclicPathTest.JavaComputerCyclicPathTest.class,
            // TODO: REMOVE? DedupTest.JavaComputerDedupTest.class
            ExceptTest.JavaComputerExceptTest.class,
            FilterTest.JavaComputerFilterTest.class,
            HasNotTest.JavaComputerHasNotTest.class,
            HasTest.JavaComputerHasTest.class,
            IntervalTest.JavaComputerIntervalTest.class,
            RandomTest.JavaComputerRandomTest.class,
            // TODO: REMOVE? RangeTest.JavaComputerRangeTest.class,
            RetainTest.JavaComputerRetainTest.class,
            SimplePathTest.JavaComputerSimplePathTest.class,


            // map
            BackTest.JavaComputerBackTest.class,
            // TODO: REMOVE? FoldTest.JavaComputerFoldTest.class,
            JumpTest.JavaComputerJumpTest.class,
            MapTest.JavaComputerMapTest.class,
            // TODO: REMOVE? OrderTest.JavaOrderTest.class
            PathTest.JavaComputerPathTest.class,
            SelectTest.JavaComputerSelectTest.class,
            TraversalTest.JavaComputerTraversalTest.class,
            UnfoldTest.JavaComputerUnfoldTest.class,
            UnionTest.JavaComputerUnionTest.class,
            ValuesTest.JavaComputerValuesTest.class,

            // sideEffect
            // TODO: REMOVE?  AddEdgeTest.JavaAddEdgeTest.class,
            AggregateTest.JavaComputerAggregateTest.class,
            CountTest.JavaComputerCountTest.class,
            GroupByTest.JavaComputerGroupByTest.class,
            GroupCountTest.JavaComputerGroupCountTest.class,
            SideEffectCapTest.JavaComputerSideEffectCapTest.class,
            // TODO: REMOVE? SideEffectTest.JavaSideEffectTest.class,
            StoreTest.JavaComputerStoreTest.class,
            // TODO: REMOVE? SubgraphTest.JavaSideEffectTest.class,
            TreeTest.JavaComputerTreeTest.class,


            // algorithms
            PageRankVertexProgramTest.class
    };

    /**
     * Tests that will be enforced by the suite where instances of them should be in the list of testsToExecute.
     */
    protected static final Class<?>[] testsToEnforce = new Class<?>[]{
            // filter
            CyclicPathTest.class,
            // DedupTest.class,
            ExceptTest.class,
            FilterTest.class,
            HasNotTest.class,
            HasTest.class,
            IntervalTest.class,
            RandomTest.class,
            // RangeTest.class,
            RetainTest.class,
            SimplePathTest.class,


            // map
            BackTest.class,
            // FoldTest.class,
            JumpTest.class,
            MapTest.class,
            // OrderTest.class,
            PathTest.class,
            SelectTest.class,
            Traversal.class,
            UnfoldTest.class,
            UnionTest.class,
            ValuesTest.class,


            // sideEffect
            // AddEdgeTest.class,
            AggregateTest.class,
            CountTest.class,
            GroupByTest.class,
            GroupCountTest.class,
            SideEffectCapTest.class,
            // SideEffectTest.class,
            StoreTest.class,
            // SubGraphTest.class,
            TreeTest.class,


            // algorithms
            PageRankVertexProgramTest.class
    };

    public ProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }

    public ProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }
}
