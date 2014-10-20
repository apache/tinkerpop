package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.process.computer.GraphComputerTest;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgramTest;
import com.tinkerpop.gremlin.process.graph.step.branch.UnionTest;
import com.tinkerpop.gremlin.process.graph.step.filter.CyclicPathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterTest;
import com.tinkerpop.gremlin.process.graph.step.filter.HasNotTest;
import com.tinkerpop.gremlin.process.graph.step.filter.HasTest;
import com.tinkerpop.gremlin.process.graph.step.filter.IntervalTest;
import com.tinkerpop.gremlin.process.graph.step.filter.RandomTest;
import com.tinkerpop.gremlin.process.graph.step.filter.SimplePathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.WhereTest;
import com.tinkerpop.gremlin.process.graph.step.map.BackTest;
import com.tinkerpop.gremlin.process.graph.step.branch.ChooseTest;
import com.tinkerpop.gremlin.process.graph.step.branch.JumpTest;
import com.tinkerpop.gremlin.process.graph.step.map.MapTest;
import com.tinkerpop.gremlin.process.graph.step.map.PathTest;
import com.tinkerpop.gremlin.process.graph.step.map.PropertiesTest;
import com.tinkerpop.gremlin.process.graph.step.map.SelectTest;
import com.tinkerpop.gremlin.process.graph.step.map.ShuffleTest;
import com.tinkerpop.gremlin.process.graph.step.map.UnfoldTest;
import com.tinkerpop.gremlin.process.graph.step.branch.UntilTest;
import com.tinkerpop.gremlin.process.graph.step.map.ValueMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.VertexTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.TreeTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The ProcessComputerStandardSuite is a custom JUnit test runner that executes the Gremlin Test Suite over a Graph
 * implementation.  This specialized test suite and runner is for use by Gremlin implementers to test their
 * Graph implementations.  The StructureStandardSuite ensures consistency and validity of the implementations that they
 * test.
 * <p/>
 * To use the ProcessComputerStandardSuite define a class in a test module.  Simple naming would expect the name of the
 * implementation followed by "ProcessComputerStandardSuite".  This class should be annotated as follows (note that the "Suite"
 * implements ProcessComputerStandardSuite.GraphProvider as a convenience only...it could be implemented in a separate class file):
 * <code>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @RunWith(ProcessComputerSuite.class)
 * @ProcessComputerSuite.GraphProviderClass(TinkerGraphProcessComputerTest.class) public class TinkerGraphProcessComputerTest implements GraphProvider {
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
    private static final Class<?>[] allTests = new Class<?>[]{
            // basic api semantics testing
            GraphComputerTest.ComputerTest.class,   // todo: not sure this should be here as it forces retest of GraphComputer without an "implementation"

            // branch
            ChooseTest.ComputerTest.class,
            JumpTest.ComputerTest.class,
            UnionTest.ComputerTest.class,
            UntilTest.ComputerTest.class,

            // filter
            CyclicPathTest.ComputerTest.class,
            // TODO: REMOVE? DedupTest.JavaComputerDedupTest.class
            // TODO: Aggregate isn't accessible // ExceptTest.JavaComputerExceptTest.class,
            FilterTest.ComputerTest.class,
            HasNotTest.ComputerTest.class,
            HasTest.ComputerTest.class,
            IntervalTest.ComputerTest.class,
            RandomTest.ComputerTest.class,
            // TODO: REMOVE? RangeTest.JavaComputerRangeTest.class,
            // TODO: Aggregate isn't accessible // RetainTest.JavaComputerRetainTest.class,
            SimplePathTest.ComputerTest.class,
            WhereTest.ComputerTest.class,

            // map
            BackTest.ComputerTest.class,
            // TODO: REMOVE? FoldTest.JavaComputerFoldTest.class,
            MapTest.ComputerTest.class,
            // TODO: MatchTest.JavaComputerMatchTest.class,
            // TODO: REMOVE? OrderTest.JavaOrderTest.class
            PathTest.ComputerTest.class,
            PropertiesTest.ComputerTest.class,
            SelectTest.ComputerTest.class,
            ShuffleTest.ComputerTest.class,
            VertexTest.ComputerTest.class,
            UnfoldTest.ComputerTest.class,
            ValueMapTest.ComputerTest.class,

            // sideEffect
            // TODO: REMOVE?  AddEdgeTest.JavaAddEdgeTest.class,
            AggregateTest.ComputerTest.class,
            CountTest.ComputerTest.class,
            GroupByTest.ComputerTest.class,
            GroupCountTest.ComputerTest.class,
            // TODO: InjectTest.JavaComputerInjectTest.class,
            ProfileTest.ComputerTest.class,
            SideEffectCapTest.ComputerTest.class,
            // TODO: REMOVE? SideEffectTest.JavaSideEffectTest.class,
            StoreTest.ComputerTest.class,
            // TODO: REMOVE? SubgraphTest.JavaSideEffectTest.class,
            TreeTest.ComputerTest.class,

            // algorithms
            PageRankVertexProgramTest.class,

            // compliance
            TraversalCoverageTest.class
    };

    /**
     * Tests that will be enforced by the suite where instances of them should be in the list of testsToExecute.
     */
    protected static final Class<?>[] testsToEnforce = new Class<?>[]{
            // basic api semantics testing
            GraphComputerTest.class,

            // filter
            CyclicPathTest.class,
            // DedupTest.class,
            // ExceptTest.class,
            FilterTest.class,
            HasNotTest.class,
            HasTest.class,
            // InjectTest.class,
            IntervalTest.class,
            RandomTest.class,
            // RangeTest.class,
            // RetainTest.class,
            SimplePathTest.class,
            WhereTest.class,


            // map
            BackTest.class,
            // FoldTest.class,
            JumpTest.class,
            MapTest.class,
            // MatchTest.class,
            // OrderTest.class,
            PathTest.class,
            SelectTest.class,
            Traversal.class,
            UnfoldTest.class,
            //UnionTest.class,
            UntilTest.class,
            ValueMapTest.class,


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
            PageRankVertexProgramTest.class,

            // compliance
            TraversalCoverageTest.class
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

    public ProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }

    public ProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }
}
