package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.process.computer.GraphComputerTest;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgramTest;
import com.tinkerpop.gremlin.process.computer.util.ComputerDataStrategyTest;
import com.tinkerpop.gremlin.process.graph.step.branch.BranchTest;
import com.tinkerpop.gremlin.process.graph.step.branch.ChooseTest;
import com.tinkerpop.gremlin.process.graph.step.branch.RepeatTest;
import com.tinkerpop.gremlin.process.graph.step.branch.UnionTest;
import com.tinkerpop.gremlin.process.graph.step.filter.BetweenTest;
import com.tinkerpop.gremlin.process.graph.step.filter.CoinTest;
import com.tinkerpop.gremlin.process.graph.step.filter.CyclicPathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.ExceptTest;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterTest;
import com.tinkerpop.gremlin.process.graph.step.filter.HasNotTest;
import com.tinkerpop.gremlin.process.graph.step.filter.HasTest;
import com.tinkerpop.gremlin.process.graph.step.filter.RetainTest;
import com.tinkerpop.gremlin.process.graph.step.filter.SampleTest;
import com.tinkerpop.gremlin.process.graph.step.filter.SimplePathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.WhereTest;
import com.tinkerpop.gremlin.process.graph.step.map.BackTest;
import com.tinkerpop.gremlin.process.graph.step.map.FoldTest;
import com.tinkerpop.gremlin.process.graph.step.map.LocalTest;
import com.tinkerpop.gremlin.process.graph.step.map.MapTest;
import com.tinkerpop.gremlin.process.graph.step.map.OrderTest;
import com.tinkerpop.gremlin.process.graph.step.map.PathTest;
import com.tinkerpop.gremlin.process.graph.step.map.PropertiesTest;
import com.tinkerpop.gremlin.process.graph.step.map.SelectTest;
import com.tinkerpop.gremlin.process.graph.step.map.ShuffleTest;
import com.tinkerpop.gremlin.process.graph.step.map.UnfoldTest;
import com.tinkerpop.gremlin.process.graph.step.map.ValueMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.VertexTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.InjectTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SackTest;
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
 * The ProcessComputerStandardSuite is a mapper JUnit test runner that executes the Gremlin Test Suite over a Graph
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
            BranchTest.ComputerTest.class,
            ChooseTest.ComputerTest.class,
            RepeatTest.ComputerTest.class,
            UnionTest.ComputerTest.class,

            // filter
            BetweenTest.ComputerTest.class,
            CyclicPathTest.ComputerTest.class,
            // TODO: DedupTest.ComputerTest.class
            ExceptTest.ComputerTest.class,
            FilterTest.ComputerTest.class,
            HasNotTest.ComputerTest.class,
            HasTest.ComputerTest.class,
            CoinTest.ComputerTest.class,
            // TODO: RangeTest.ComputerTest.class,
            RetainTest.ComputerTest.class,
            SampleTest.ComputerTest.class,
            SimplePathTest.ComputerTest.class,
            WhereTest.ComputerTest.class,

            // map
            BackTest.ComputerTest.class,
            FoldTest.ComputerTest.class,
            LocalTest.ComputerTest.class,
            MapTest.ComputerTest.class,
            // TODO: MatchTest.ComputerTest.class,
            // TODO: OrderByTest.ComputerTest.class
            OrderTest.ComputerTest.class,
            PathTest.ComputerTest.class,
            PropertiesTest.ComputerTest.class,
            SelectTest.ComputerTest.class,
            ShuffleTest.ComputerTest.class,
            UnfoldTest.ComputerTest.class,
            ValueMapTest.ComputerTest.class,
            VertexTest.ComputerTest.class,

            // sideEffect
            // TODO: AddEdgeTest.ComputerTest.class,
            AggregateTest.ComputerTest.class,
            CountTest.ComputerTest.class,
            GroupTest.ComputerTest.class,
            GroupCountTest.ComputerTest.class,
            // TODO: InjectTest.ComputerTest.class,
            ProfileTest.ComputerTest.class,
            SackTest.ComputerTest.class,
            SideEffectCapTest.ComputerTest.class,
            // TODO: REMOVE? SideEffectTest.ComputerTest.class,
            StoreTest.ComputerTest.class,
            // TODO: REMOVE? SubgraphTest.ComputerTest.class,
            TreeTest.ComputerTest.class,

            // algorithms
            PageRankVertexProgramTest.class,

            // compliance
            GraphTraversalCoverageTest.class,

            // strategy
            ComputerDataStrategyTest.class
    };

    /**
     * Tests that will be enforced by the suite where instances of them should be in the list of testsToExecute.
     */
    protected static final Class<?>[] testsToEnforce = new Class<?>[]{
            // basic api semantics testing
            GraphComputerTest.class,

            // branch
            BranchTest.class,
            ChooseTest.class,
            RepeatTest.class,
            UnionTest.class,

            // filter
            BetweenTest.class,
            CyclicPathTest.class,
            // DedupTest.class,
            ExceptTest.class,
            FilterTest.class,
            HasNotTest.class,
            HasTest.class,
            CoinTest.class,
            // RangeTest.class,
            RetainTest.class,
            SampleTest.class,
            SimplePathTest.class,
            WhereTest.class,


            // map
            BackTest.class,
            // FoldTest.class,
            LocalTest.class,
            MapTest.class,
            // MatchTest.class,
            // OrderByTest.class,
            OrderTest.class,
            PathTest.class,
            PropertiesTest.class,
            SelectTest.class,
            ShuffleTest.class,
            UnfoldTest.class,
            ValueMapTest.class,
            VertexTest.class,


            // sideEffect
            // AddEdgeTest.class,
            AggregateTest.class,
            CountTest.class,
            GroupTest.class,
            GroupCountTest.class,
            InjectTest.class,
            ProfileTest.class,
            SackTest.class,
            SideEffectCapTest.class,
            // SideEffectTest.class,
            StoreTest.class,
            // SubgraphTest.class,
            TreeTest.class,


            // algorithms
            PageRankVertexProgramTest.class,

            // compliance
            GraphTraversalCoverageTest.class,

            // strategy
            ComputerDataStrategyTest.class
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
