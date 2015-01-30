package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.BranchTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.ChooseTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.LocalTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.RepeatTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.UnionTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.AndTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.CoinTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.CyclicPathTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.DedupTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.ExceptTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.FilterTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.HasNotTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.HasTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.IsTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.OrTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.RangeTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.RetainTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.SampleTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.SimplePathTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.WhereTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.BackTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.CountTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.FoldTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.MapTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.MatchTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.MaxTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.MeanTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.MinTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.OrderTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.PropertiesTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.SelectTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.UnfoldTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.ValueMapTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.VertexTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.AddEdgeTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.AggregateTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroupCountTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroupTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.InjectTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.ProfileTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.SackTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.SideEffectCapTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.SideEffectTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.StoreTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.SubgraphTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.TreeTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.TraversalSideEffectsTest;
import com.tinkerpop.gremlin.process.graph.traversal.strategy.TraversalVerificationStrategyTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The ProcessStandardSuite is a mapper JUnit test runner that executes the Gremlin Test Suite over a Graph
 * implementation.  This specialized test suite and runner is for use by Gremlin implementers to test their
 * Graph implementations.  The StructureStandardSuite ensures consistency and validity of the implementations that they
 * test.
 * <p/>
 * To use the ProcessStandardSuite define a class in a test module.  Simple naming would expect the name of the
 * implementation followed by "ProcessStandardSuite".  This class should be annotated as follows (note that the "Suite"
 * implements ProcessStandardSuite.GraphProvider as a convenience only...it could be implemented in a separate class file):
 * <p/>
 * <code>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @RunWith(ProcessStandardSuite.class)
 * @ProcessStandardSuite.GraphProviderClass(TinkerGraphProcessStandardTest.class) public class TinkerGraphProcessStandardTest implements GraphProvider {
 * }
 * </code>
 * <p/>
 * Implementing {@link com.tinkerpop.gremlin.GraphProvider} provides a way for the ProcessStandardSuite to
 * instantiate Graph instances from the implementation being tested to inject into tests in the suite.  The
 * ProcessStandardSuite will utilized Features defined in the suite to determine which tests will be executed.
 */
public class ProcessStandardSuite extends AbstractGremlinSuite {

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] allTests = new Class<?>[]{
            // branch
            BranchTest.StandardTest.class,
            ChooseTest.StandardTest.class,
            LocalTest.StandardTest.class,
            RepeatTest.StandardTest.class,
            UnionTest.StandardTest.class,

            // filter
            AndTest.StandardTest.class,
            CoinTest.StandardTest.class,
            CyclicPathTest.StandardTest.class,
            DedupTest.StandardTest.class,
            ExceptTest.StandardTest.class,
            FilterTest.StandardTest.class,
            HasNotTest.StandardTest.class,
            HasTest.StandardTest.class,
            IsTest.StandardTest.class,
            OrTest.StandardTest.class,
            RangeTest.StandardTest.class,
            RetainTest.StandardTest.class,
            SampleTest.StandardTest.class,
            SimplePathTest.StandardTest.class,
            WhereTest.StandardTest.class,

            // map
            BackTest.StandardTest.class,
            CountTest.StandardTest.class,
            FoldTest.StandardTest.class,
            MapTest.StandardTest.class,
            MatchTest.StandardTest.class,
            MaxTest.StandardTest.class,
            MeanTest.StandardTest.class,
            MinTest.StandardTest.class,
            OrderTest.StandardTest.class,
            com.tinkerpop.gremlin.process.graph.traversal.step.map.PathTest.StandardTest.class,
            PropertiesTest.StandardTest.class,
            SelectTest.StandardTest.class,
            VertexTest.StandardTest.class,
            UnfoldTest.StandardTest.class,
            ValueMapTest.StandardTest.class,

            // sideEffect
            AddEdgeTest.StandardTest.class,
            AggregateTest.StandardTest.class,
            GroupTest.StandardTest.class,
            GroupCountTest.StandardTest.class,
            InjectTest.StandardTest.class,
            ProfileTest.StandardTest.class,
            SackTest.StandardTest.class,
            SideEffectCapTest.StandardTest.class,
            SideEffectTest.StandardTest.class,
            StoreTest.StandardTest.class,
            SubgraphTest.StandardTest.class,
            TreeTest.StandardTest.class,

            // util
            TraversalSideEffectsTest.StandardTest.class,

            // compliance
            GraphTraversalCoverageTest.class,
            CoreTraversalTest.class,
            PathTest.class,

            // strategy
            TraversalVerificationStrategyTest.StandardTest.class

            // algorithms
            // PageRankVertexProgramTest.class
    };

    /**
     * Tests that will be enforced by the suite where instances of them should be in the list of testsToExecute.
     */
    protected static Class<?>[] testsToEnforce = new Class<?>[]{
            // branch
            BranchTest.class,
            ChooseTest.class,
            LocalTest.class,
            RepeatTest.class,
            UnionTest.class,

            // filter
            AndTest.class,
            CoinTest.class,
            CyclicPathTest.class,
            DedupTest.class,
            ExceptTest.class,
            FilterTest.class,
            HasNotTest.class,
            HasTest.class,
            IsTest.class,
            OrTest.class,
            RangeTest.class,
            RetainTest.class,
            SampleTest.class,
            SimplePathTest.class,
            WhereTest.class,

            // map
            BackTest.class,
            CountTest.class,
            FoldTest.class,
            MapTest.class,
            MatchTest.class,
            MaxTest.class,
            MeanTest.class,
            MinTest.class,
            OrderTest.class,
            com.tinkerpop.gremlin.process.graph.traversal.step.map.PathTest.class,
            SelectTest.class,
            VertexTest.class,
            UnfoldTest.class,
            ValueMapTest.class,

            // sideEffect
            AddEdgeTest.class,
            AggregateTest.class,
            GroupTest.class,
            GroupCountTest.class,
            InjectTest.class,
            ProfileTest.class,
            SackTest.class,
            SideEffectCapTest.class,
            SideEffectTest.class,
            StoreTest.class,
            SubgraphTest.class,
            TreeTest.class,

            // util
            TraversalSideEffectsTest.class,

            // compliance
            GraphTraversalCoverageTest.class,
            CoreTraversalTest.class,
            PathTest.class,

            // algorithms
            // PageRankVertexProgramTest.class

            // strategy
            TraversalVerificationStrategyTest.class
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
            testsToEnforce = testsToExecute;
        }
    }

    public ProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }

    public ProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }

    public ProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce, final boolean gremlinFlavorSuite) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce, gremlinFlavorSuite);
    }
}
