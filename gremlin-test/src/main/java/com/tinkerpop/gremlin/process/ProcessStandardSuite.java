package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.process.graph.step.branch.ChooseTest;
import com.tinkerpop.gremlin.process.graph.step.branch.JumpTest;
import com.tinkerpop.gremlin.process.graph.step.branch.UnionTest;
import com.tinkerpop.gremlin.process.graph.step.branch.UntilTest;
import com.tinkerpop.gremlin.process.graph.step.filter.CyclicPathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.DedupTest;
import com.tinkerpop.gremlin.process.graph.step.filter.ExceptTest;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterTest;
import com.tinkerpop.gremlin.process.graph.step.filter.HasNotTest;
import com.tinkerpop.gremlin.process.graph.step.filter.HasTest;
import com.tinkerpop.gremlin.process.graph.step.filter.IntervalTest;
import com.tinkerpop.gremlin.process.graph.step.filter.LocalRangeTest;
import com.tinkerpop.gremlin.process.graph.step.filter.RandomTest;
import com.tinkerpop.gremlin.process.graph.step.filter.RangeTest;
import com.tinkerpop.gremlin.process.graph.step.filter.RetainTest;
import com.tinkerpop.gremlin.process.graph.step.filter.SimplePathTest;
import com.tinkerpop.gremlin.process.graph.step.filter.WhereTest;
import com.tinkerpop.gremlin.process.graph.step.map.BackTest;
import com.tinkerpop.gremlin.process.graph.step.map.FoldTest;
import com.tinkerpop.gremlin.process.graph.step.map.HiddenValueMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.MapTest;
import com.tinkerpop.gremlin.process.graph.step.map.MatchTest;
import com.tinkerpop.gremlin.process.graph.step.map.OrderByTest;
import com.tinkerpop.gremlin.process.graph.step.map.OrderTest;
import com.tinkerpop.gremlin.process.graph.step.map.PathTest;
import com.tinkerpop.gremlin.process.graph.step.map.PropertiesTest;
import com.tinkerpop.gremlin.process.graph.step.map.SelectTest;
import com.tinkerpop.gremlin.process.graph.step.map.ShuffleTest;
import com.tinkerpop.gremlin.process.graph.step.map.UnfoldTest;
import com.tinkerpop.gremlin.process.graph.step.map.ValueMapTest;
import com.tinkerpop.gremlin.process.graph.step.map.VertexTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AddEdgeTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.InjectTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SubgraphTest;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.TreeTest;
import com.tinkerpop.gremlin.process.graph.step.util.TraversalSideEffectsTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The ProcessStandardSuite is a custom JUnit test runner that executes the Gremlin Test Suite over a Graph
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
            ChooseTest.StandardTest.class,
            JumpTest.StandardTest.class,
            UnionTest.StandardTest.class,
            UntilTest.StandardTest.class,

            // filter
            CyclicPathTest.StandardTest.class,
            DedupTest.StandardTest.class,
            ExceptTest.StandardTest.class,
            FilterTest.StandardTest.class,
            HasNotTest.StandardTest.class,
            HasTest.StandardTest.class,
            IntervalTest.StandardTest.class,
            LocalRangeTest.StandardTest.class,
            RandomTest.StandardTest.class,
            RangeTest.StandardTest.class,
            RetainTest.StandardTest.class,
            SimplePathTest.StandardTest.class,
            WhereTest.StandardTest.class,

            // map
            BackTest.StandardTest.class,
            FoldTest.StandardTest.class,
            HiddenValueMapTest.StandardTest.class,
            MapTest.StandardTest.class,
            MatchTest.StandardTest.class,
            OrderTest.StandardTest.class,
            OrderByTest.StandardTest.class,
            PathTest.StandardTest.class,
            PropertiesTest.StandardTest.class,
            SelectTest.StandardTest.class,
            ShuffleTest.StandardTest.class,
            VertexTest.StandardTest.class,
            UnfoldTest.StandardTest.class,
            ValueMapTest.StandardTest.class,

            // sideEffect
            AddEdgeTest.StandardTest.class,
            AggregateTest.StandardTest.class,
            CountTest.StandardTest.class,
            GroupByTest.StandardTest.class,
            GroupCountTest.StandardTest.class,
            InjectTest.StandardTest.class,
            ProfileTest.StandardTest.class,
            SideEffectCapTest.StandardTest.class,
            SideEffectTest.StandardTest.class,
            StoreTest.StandardTest.class,
            SubgraphTest.StandardTest.class,
            TreeTest.StandardTest.class,

            // util
            TraversalSideEffectsTest.StandardTest.class,

            // compliance
            TraversalCoverageTest.class,
            CoreTraversalTest.class,
            PathStructureTest.class,

            // algorithms
            // PageRankVertexProgramTest.class
    };

    /**
     * Tests that will be enforced by the suite where instances of them should be in the list of testsToExecute.
     */
    protected static Class<?>[] testsToEnforce = new Class<?>[]{
            // branch
            ChooseTest.class,
            JumpTest.class,
            UnionTest.class,
            UntilTest.class,

            // filter
            CyclicPathTest.class,
            DedupTest.class,
            ExceptTest.class,
            FilterTest.class,
            HasNotTest.class,
            HasTest.class,
            IntervalTest.class,
            RandomTest.class,
            RangeTest.class,
            RetainTest.class,
            SimplePathTest.class,
            WhereTest.class,

            // map
            BackTest.class,
            FoldTest.class,
            HiddenValueMapTest.class,
            MapTest.class,
            MatchTest.class,
            OrderTest.class,
            OrderByTest.class,
            PathTest.class,
            SelectTest.class,
            ShuffleTest.class,
            VertexTest.class,
            UnfoldTest.class,
            ValueMapTest.class,

            // sideEffect
            AddEdgeTest.class,
            AggregateTest.class,
            CountTest.class,
            GroupByTest.class,
            GroupCountTest.class,
            InjectTest.class,
            ProfileTest.class,
            SideEffectCapTest.class,
            SideEffectTest.class,
            StoreTest.class,
            SubgraphTest.class,
            TreeTest.class,

            // util
            TraversalSideEffectsTest.class,

            // compliance
            TraversalCoverageTest.class,
            CoreTraversalTest.class,
            PathStructureTest.class,

            // algorithms
            // PageRankVertexProgramTest.class
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
