package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Pair;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_PROPERTIES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures.FEATURE_VARIABLES;
import static org.junit.Assert.*;

/**
 * Tests that ensure proper wrapping of {@link com.tinkerpop.gremlin.structure.Graph} classes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class StrategyGraphTest {

    public static class CoreTest extends AbstractGremlinTest {
        @Test(expected = IllegalArgumentException.class)
        public void shouldNotAllowAStrategyWrappedGraphToBeReWrapped() {
            final StrategyGraph swg = new StrategyGraph(g);
            new StrategyGraph(swg);
        }

        @Test(expected = IllegalArgumentException.class)
        public void shouldNotAllowAStrategyWrappedGraphToBeReWrappedViaStrategy() {
            final StrategyGraph swg = new StrategyGraph(g);
            swg.strategy(IdentityStrategy.instance());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveGraphWrappedFromVertex() {
            final StrategyGraph swg = new StrategyGraph(g);
            assertTrue(swg.addVertex().graph() instanceof StrategyGraph);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldHaveGraphWrappedFromEdge() {
            final StrategyGraph swg = new StrategyGraph(g);
            final Vertex v = swg.addVertex();
            assertTrue(v.addEdge("self", v).graph() instanceof StrategyGraph);
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_ADD_PROPERTY)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void shouldHaveGraphWrappedFromVertexProperty() {
            final StrategyGraph swg = new StrategyGraph(g);
            assertTrue(swg.addVertex().property("name", "stephen").graph() instanceof StrategyGraph);
        }
    }

    @RunWith(Parameterized.class)
    public static class ToStringConsistencyTest extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            return new ArrayList<Object[]>() {{
                add(new Object[]{IdentityStrategy.instance()});
                add(new Object[]{IdStrategy.build("key").create()});
                add(new Object[]{PartitionStrategy.<PartitionStrategy.Builder>build().partitionKey("partition").startPartition("A").create()});
                add(new Object[]{ReadOnlyStrategy.instance()});
                add(new Object[]{SequenceStrategy.build().sequence(ReadOnlyStrategy.instance(), PartitionStrategy.build().partitionKey("partition").startPartition("A").create()).create()});
                add(new Object[]{SubgraphStrategy.build().create()});
            }};
        }

        @Parameterized.Parameter(value = 0)
        public GraphStrategy strategy;

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldReturnWrappedVertexToString() {
            final StrategyGraph swg = new StrategyGraph(g);
            final StrategyVertex v1 = (StrategyVertex) swg.addVertex(T.label, "Person");
            assertEquals(StringFactory.graphStrategyElementString(v1), v1.toString());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldReturnWrappedEdgeToString() {
            final StrategyGraph swg = new StrategyGraph(g);
            final Vertex v1 = swg.addVertex(T.label, "Person");
            final Vertex v2 = swg.addVertex(T.label, "Person");
            final StrategyEdge e1 = (StrategyEdge) v1.addEdge("friend", v2);
            assertEquals(StringFactory.graphStrategyElementString(e1), e1.toString());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldReturnWrappedVertexPropertyToString() {
            final StrategyGraph swg = new StrategyGraph(g);
            final Vertex v1 = swg.addVertex(T.label, "Person", "age", "one");
            final StrategyVertexProperty age = (StrategyVertexProperty) v1.property("age");
            assertEquals(StringFactory.graphStrategyElementString(age), age.toString());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldReturnWrappedPropertyToString() {
            final StrategyGraph swg = new StrategyGraph(g);
            final Vertex v1 = swg.addVertex(T.label, "Person");
            final Vertex v2 = swg.addVertex(T.label, "Person");
            final Edge e1 = v1.addEdge("friend", v2, "weight", "fifty");
            final StrategyProperty weight = (StrategyProperty) e1.property("weight");
            assertEquals(StringFactory.graphStrategyPropertyString(weight), weight.toString());
        }

        @Test
        public void shouldReturnWrappedGraphToString() {
            final StrategyGraph swg = new StrategyGraph(g);
            final GraphStrategy strategy = swg.getStrategy();
            assertNotEquals(g.toString(), swg.toString());
            assertEquals(StringFactory.graphStrategyString(strategy, g), swg.toString());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VariableFeatures.class, feature = FEATURE_VARIABLES)
        public void shouldReturnWrappedVariablesToString() {
            final StrategyGraph swg = new StrategyGraph(g);
            final StrategyVariables variables = (StrategyVariables) swg.variables();
            assertEquals(StringFactory.graphStrategyVariables(variables), variables.toString());
        }
    }

    public static class BlockBaseFunctionTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldNotCallBaseFunctionThusNotRemovingTheVertex() throws Exception {
            // create an ad-hoc strategy that only marks a vertex as "deleted" and removes all edges and properties
            // but doesn't actually blow it away
            final StrategyGraph swg = g.strategy(new GraphStrategy() {
                @Override
                public UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
                    return (t) -> () -> {
                        final Vertex v = ctx.getCurrent().getBaseVertex();
                        v.bothE().remove();
                        v.properties().forEachRemaining(Property::remove);
                        v.property("deleted", true);
                        return null;
                    };
                }
            });

            final Vertex toRemove = g.addVertex("name", "pieter");
            toRemove.addEdge("likes", g.addVertex("feature", "Strategy"));

            assertEquals(1, toRemove.properties().count().next().intValue());
            assertEquals(new Long(1), toRemove.bothE().count().next());
            assertFalse(toRemove.property("deleted").isPresent());

            swg.V(toRemove.id()).remove();

            final Vertex removed = g.V(toRemove.id()).next();
            assertNotNull(removed);
            assertEquals(1, removed.properties().count().next().intValue());
            assertEquals(new Long(0), removed.bothE().count().next());
            assertTrue(removed.property("deleted").isPresent());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldNotCallBaseFunctionThusNotRemovingTheEdge() throws Exception {
            // create an ad-hoc strategy that only marks a vertex as "deleted" and removes all edges and properties
            // but doesn't actually blow it away
            final StrategyGraph swg = g.strategy(new GraphStrategy() {
                @Override
                public UnaryOperator<Supplier<Void>> getRemoveEdgeStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
                    return (t) -> () -> {
                        final Edge e = ctx.getCurrent().getBaseEdge();
                        e.properties().forEachRemaining(Property::remove);
                        e.property("deleted", true);
                        return null;
                    };
                }
            });

            final Vertex v = g.addVertex("name", "pieter");
            final Edge e = v.addEdge("likes", g.addVertex("feature", "Strategy"), "this", "something");

            assertEquals(1, e.properties().count().next().intValue());
            assertFalse(e.property("deleted").isPresent());

            swg.E(e.id()).remove();

            final Edge removed = g.E(e.id()).next();
            assertNotNull(removed);
            assertEquals(1, removed.properties().count().next().intValue());
            assertTrue(removed.property("deleted").isPresent());
        }

        @Test
        public void shouldAdHocTheCloseStrategy() throws Exception {
            final AtomicInteger counter = new AtomicInteger(0);
            final StrategyGraph swg = g.strategy(new GraphStrategy() {
                @Override
                public UnaryOperator<Supplier<Void>> getGraphCloseStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
                    return (t) -> () -> {
                        counter.incrementAndGet();
                        return null;
                    };
                }
            });

            // allows multiple calls to close() - the test will clean up with a call to the base graph.close()
            swg.close();
            swg.close();
            swg.close();

            assertEquals(3, counter.get());
        }
    }

    @RunWith(Parameterized.class)
    public static class EdgePropertyShouldBeWrappedTests extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, Edge, Stream<Property<Object>>>>> tests = new ArrayList<>();
            tests.add(Pair.with("e.property(all)", (Graph g, Edge e) -> Stream.of(e.property("all"))));
            tests.add(Pair.with("e.iterators().properties()", (Graph g, Edge e) -> StreamFactory.stream(e.iterators().propertyIterator())));
            tests.add(Pair.with("e.iterators().properties(any)", (Graph g, Edge e) -> StreamFactory.stream(e.iterators().propertyIterator("any"))));
            tests.add(Pair.with("e.properties()", (Graph g, Edge e) -> StreamFactory.stream(e.properties())));
            tests.add(Pair.with("e.property(extra,more)", (Graph g, Edge e) -> Stream.<Property<Object>>of(e.property("extra", "more"))));

            return tests.stream().map(d -> {
                final Object[] o = new Object[2];
                o[0] = d.getValue0();
                o[1] = d.getValue1();
                return o;
            }).collect(Collectors.toList());
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public BiFunction<Graph, Edge, Stream<? extends Property<Object>>> streamGetter;

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldWrap() {
            final StrategyGraph swg = g.strategy(IdentityStrategy.instance());
            final Vertex v = swg.addVertex();
            final Edge e = v.addEdge("to", v, "all", "a", "any", "something", "hideme", "hidden");

            final AtomicBoolean atLeastOne = new AtomicBoolean(false);
            assertTrue(streamGetter.apply(swg, e).allMatch(p -> {
                atLeastOne.set(true);
                return p instanceof StrategyProperty;
            }));

            assertTrue(atLeastOne.get());
        }
    }

    @RunWith(Parameterized.class)
    public static class VertexPropertyPropertyShouldBeWrappedTests extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, VertexProperty, Stream<Property<Object>>>>> tests = new ArrayList<>();
            tests.add(Pair.with("vp.property(food)", (Graph g, VertexProperty vp) -> Stream.of(vp.property("food"))));
            tests.add(Pair.with("vp.property(moreFood,sandwhich)", (Graph g, VertexProperty vp) -> Stream.of(vp.property("moreFood", "sandwhich"))));
            tests.add(Pair.with("vp.iterators().properties()", (Graph g, VertexProperty vp) -> StreamFactory.stream(vp.iterators().propertyIterator())));
            tests.add(Pair.with("vp.iterators().properties(food)", (Graph g, VertexProperty vp) -> StreamFactory.stream(vp.iterators().propertyIterator("food"))));
            tests.add(Pair.with("vp.propertyMap().next().values()", (Graph g, VertexProperty vp) -> StreamFactory.stream(vp.propertyMap().next().values())));

            return tests.stream().map(d -> {
                final Object[] o = new Object[2];
                o[0] = d.getValue0();
                o[1] = d.getValue1();
                return o;
            }).collect(Collectors.toList());
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public BiFunction<Graph, VertexProperty, Stream<? extends Property<Object>>> streamGetter;

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
        public void shouldWrap() {
            final StrategyGraph swg = g.strategy(IdentityStrategy.instance());
            final Vertex v = swg.addVertex();
            final VertexProperty vp = v.property("property", "on-property", "food", "taco", "more", "properties");

            final AtomicBoolean atLeastOne = new AtomicBoolean(false);
            assertTrue(streamGetter.apply(swg, vp).allMatch(p -> {
                atLeastOne.set(true);
                return p instanceof StrategyProperty;
            }));

            assertTrue(atLeastOne.get());
        }
    }

    @RunWith(Parameterized.class)
    public static class VertexPropertyShouldBeWrappedTest extends AbstractGremlinTest {
        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, Vertex, Stream<VertexProperty<Object>>>>> tests = new ArrayList<>();
            tests.add(Pair.with("v.property(all)", (Graph g, Vertex v) -> Stream.of(v.property("all"))));
            tests.add(Pair.with("v.property(extra, data)", (Graph g, Vertex v) -> Stream.of(v.<Object>property("extra", "data"))));
            tests.add(Pair.with("v.iterators().properties()", (Graph g, Vertex v) -> StreamFactory.stream(v.iterators().propertyIterator())));
            tests.add(Pair.with("v.iterators().properties(any)", (Graph g, Vertex v) -> StreamFactory.stream(v.iterators().propertyIterator("any"))));
            tests.add(Pair.with("v.properties()", (Graph g, Vertex v) -> StreamFactory.stream(v.properties())));
            tests.add(Pair.with("v.property(extra,more)", (Graph g, Vertex v) -> Stream.<VertexProperty<Object>>of(v.property("extra", "more"))));

            return tests.stream().map(d -> {
                final Object[] o = new Object[2];
                o[0] = d.getValue0();
                o[1] = d.getValue1();
                return o;
            }).collect(Collectors.toList());
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public BiFunction<Graph, Vertex, Stream<? extends Property<Object>>> streamGetter;

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldWrap() {
            final StrategyGraph swg = g.strategy(IdentityStrategy.instance());
            final Vertex v = swg.addVertex("all", "a", "any", "something", "hideme", "hidden");

            final AtomicBoolean atLeastOne = new AtomicBoolean(false);
            assertTrue(streamGetter.apply(g, v).allMatch(p -> {
                atLeastOne.set(true);
                return p instanceof StrategyVertexProperty;
            }));

            assertTrue(atLeastOne.get());
        }
    }

    @RunWith(Parameterized.class)
    public static class VertexPropertyWithMultiPropertyShouldBeWrappedTest extends AbstractGremlinTest {
        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, Vertex, Stream<VertexProperty<Object>>>>> tests = new ArrayList<>();
            tests.add(Pair.with("v.property(all)", (Graph g, Vertex v) -> Stream.of(v.property("all"))));
            tests.add(Pair.with("v.property(extra, data)", (Graph g, Vertex v) -> Stream.of(v.<Object>property("extra", "data"))));
            tests.add(Pair.with("v.iterators().properties()", (Graph g, Vertex v) -> StreamFactory.stream(v.iterators().propertyIterator())));
            tests.add(Pair.with("v.iterators().properties(any)", (Graph g, Vertex v) -> StreamFactory.stream(v.iterators().propertyIterator("any"))));
            tests.add(Pair.with("v.properties()", (Graph g, Vertex v) -> StreamFactory.stream(v.properties())));
            tests.add(Pair.with("v.property(extra,more)", (Graph g, Vertex v) -> Stream.<VertexProperty<Object>>of(v.property("extra", "more"))));

            return tests.stream().map(d -> {
                final Object[] o = new Object[2];
                o[0] = d.getValue0();
                o[1] = d.getValue1();
                return o;
            }).collect(Collectors.toList());
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public BiFunction<Graph, Vertex, Stream<? extends Property<Object>>> streamGetter;

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        public void shouldWrap() {
            final StrategyGraph swg = g.strategy(IdentityStrategy.instance());
            final Vertex v = swg.addVertex("all", "a", "any", "something", "any", "something-else", "hideme", "hidden", "hideme", "hidden-too");

            final AtomicBoolean atLeastOne = new AtomicBoolean(false);
            assertTrue(streamGetter.apply(g, v).allMatch(p -> {
                atLeastOne.set(true);
                return p instanceof StrategyVertexProperty;
            }));

            assertTrue(atLeastOne.get());
        }
    }

    @RunWith(Parameterized.class)
    public static class EdgeShouldBeWrappedTest extends AbstractGremlinTest {
        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, AbstractGremlinTest, Stream<Edge>>>> tests = new ArrayList<>();
            tests.add(Pair.with("g.E()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.E())));
            tests.add(Pair.with("g.iterators().edgeIterator()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.iterators().edgeIterator())));
            tests.add(Pair.with("g.iterators().edgeIterator(11)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.iterators().edgeIterator(instance.convertToEdgeId("josh", "created", "lop")))));
            tests.add(Pair.with("g.E(11)", (Graph g, AbstractGremlinTest instance) -> Stream.of(g.E(instance.convertToEdgeId("josh", "created", "lop")).next())));
            tests.add(Pair.with("g.V(1).outE()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("marko")).outE())));
            tests.add(Pair.with("g.V(4).bothE()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("josh")).bothE())));
            tests.add(Pair.with("g.V(4).inE()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("josh")).inE())));
            tests.add(Pair.with("g.V(11).property(weight).element()", (Graph g, AbstractGremlinTest instance) -> Stream.of((Edge) g.E(instance.convertToEdgeId("josh", "created", "lop")).next().property("weight").element())));
            tests.add(Pair.with("g.V(4).next().iterators().edgeIterator(Direction.BOTH)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("josh")).next().iterators().edgeIterator(Direction.BOTH))));
            tests.add(Pair.with("g.V(1).next().iterators().edgeIterator(Direction.OUT)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("marko")).next().iterators().edgeIterator(Direction.OUT))));
            tests.add(Pair.with("g.V(4).next().iterators().edgeIterator(Direction.IN)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("josh")).next().iterators().edgeIterator(Direction.IN))));

            return tests.stream().map(d -> {
                final Object[] o = new Object[2];
                o[0] = d.getValue0();
                o[1] = d.getValue1();
                return o;
            }).collect(Collectors.toList());
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public BiFunction<Graph, AbstractGremlinTest, Stream<Edge>> streamGetter;

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldWrap() {
            final StrategyGraph swg = g.strategy(IdentityStrategy.instance());

            final AtomicBoolean atLeastOne = new AtomicBoolean(false);
            assertTrue(streamGetter.apply(swg, this).allMatch(e -> {
                atLeastOne.set(true);
                return e instanceof StrategyEdge;
            }));

            assertTrue(atLeastOne.get());
        }
    }

    @RunWith(Parameterized.class)
    public static class VertexShouldBeWrappedTest extends AbstractGremlinTest {
        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, AbstractGremlinTest, Stream<Vertex>>>> tests = new ArrayList<>();
            tests.add(Pair.with("g.V()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V())));
            tests.add(Pair.with("g.iterators().vertexIterator()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.iterators().vertexIterator())));
            tests.add(Pair.with("g.iterators().vertexIterator(1)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.iterators().vertexIterator(instance.convertToVertexId("marko")))));
            tests.add(Pair.with("g.V(1)", (Graph g, AbstractGremlinTest instance) -> Stream.of(g.V(instance.convertToVertexId("marko")).next())));
            tests.add(Pair.with("g.V(1).outE().inV()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("marko")).outE().inV())));
            tests.add(Pair.with("g.V(4).bothE().bothV()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("josh")).bothE().bothV())));
            tests.add(Pair.with("g.V(4).inE().outV()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("josh")).inE().outV())));
            tests.add(Pair.with("g.V(1).out()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("marko")).out())));
            tests.add(Pair.with("g.V(4).iterators().vertexIterator(Direction.BOTH)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("josh")).next().iterators().vertexIterator(Direction.BOTH))));
            tests.add(Pair.with("g.V(1).iterators().vertexIterator(Direction.OUT)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("marko")).next().iterators().vertexIterator(Direction.OUT))));
            tests.add(Pair.with("g.V(4).iterators().vertexIterator(Direction.IN)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("josh")).next().iterators().vertexIterator(Direction.IN))));
            tests.add(Pair.with("g.V(4).iterators().vertexIterator(Direction.BOTH, \"created\")", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("josh")).next().iterators().vertexIterator(Direction.BOTH, "created"))));
            tests.add(Pair.with("g.E(11).iterators().vertexIterator(Direction.IN)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.E(instance.convertToEdgeId("josh", "created", "lop")).next().iterators().vertexIterator(Direction.IN))));
            tests.add(Pair.with("g.E(11).iterators().vertexIterator(Direction.OUT)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.E(instance.convertToEdgeId("josh", "created", "lop")).next().iterators().vertexIterator(Direction.OUT))));
            tests.add(Pair.with("g.E(11).iterators().vertexIterator(Direction.BOTH)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.E(instance.convertToEdgeId("josh", "created", "lop")).next().iterators().vertexIterator(Direction.BOTH))));
            tests.add(Pair.with("g.V(1).iterators().properties(name).next().element()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("marko")).next().iterators().propertyIterator("name").next().element())));
            tests.add(Pair.with("g.V(1).outE().otherV()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("marko")).outE().otherV())));
            tests.add(Pair.with("g.V(4).inE().otherV()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("josh")).inE().otherV())));

            return tests.stream().map(d -> {
                final Object[] o = new Object[2];
                o[0] = d.getValue0();
                o[1] = d.getValue1();
                return o;
            }).collect(Collectors.toList());
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public BiFunction<Graph, AbstractGremlinTest, Stream<Vertex>> streamGetter;

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
        public void shouldWrap() {
            final StrategyGraph swg = g.strategy(IdentityStrategy.instance());

            final AtomicBoolean atLeastOne = new AtomicBoolean(false);
            assertTrue(streamGetter.apply(swg, this).allMatch(v -> {
                atLeastOne.set(true);
                return v instanceof StrategyVertex;
            }));

            assertTrue(atLeastOne.get());
        }
    }

    @RunWith(Parameterized.class)
    public static class GraphShouldBeWrappedTest extends AbstractGremlinTest {
        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, AbstractGremlinTest, Stream<Graph>>>> tests = new ArrayList<>();
            tests.add(Pair.with("g.V(1).graph()", (Graph g, AbstractGremlinTest instance) -> Stream.of(g.V(instance.convertToVertexId("marko")).next().graph())));
            tests.add(Pair.with("g.V(1).iterators().edgeIterator(BOTH).map(Edge::graph)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("marko")).next().iterators().edgeIterator(Direction.BOTH)).map(Edge::graph)));
            tests.add(Pair.with("g.V(1).iterators().propertyIterator().map(Edge::graph)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V(instance.convertToVertexId("marko")).next().iterators().propertyIterator()).map(VertexProperty::graph)));

            return tests.stream().map(d -> {
                final Object[] o = new Object[2];
                o[0] = d.getValue0();
                o[1] = d.getValue1();
                return o;
            }).collect(Collectors.toList());
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public BiFunction<Graph, AbstractGremlinTest, Stream<Graph>> streamGetter;

        @Test
        @LoadGraphWith(LoadGraphWith.GraphData.CREW)
        public void shouldWrap() {
            final StrategyGraph swg = g.strategy(IdentityStrategy.instance());

            final AtomicBoolean atLeastOne = new AtomicBoolean(false);
            assertTrue(streamGetter.apply(swg, this).allMatch(v -> {
                atLeastOne.set(true);
                return v instanceof StrategyGraph;
            }));

            assertTrue(atLeastOne.get());
        }
    }
}
