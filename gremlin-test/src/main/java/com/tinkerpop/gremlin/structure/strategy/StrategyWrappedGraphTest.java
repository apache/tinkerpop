package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * Tests that ensure proper wrapping of {@link com.tinkerpop.gremlin.structure.Graph} classes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class StrategyWrappedGraphTest  {

    public static class BlockBaseFunctionTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldNotCallBaseFunctionThusNotRemovingTheVertex() throws Exception {
            final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);

            // create an ad-hoc strategy that only marks a vertex as "deleted" and removes all edges and properties
            // but doesn't actually blow it away
            swg.strategy().setGraphStrategy(new GraphStrategy() {
                @Override
                public UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
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

            swg.v(toRemove.id()).remove();

            final Vertex removed = g.v(toRemove.id());
            assertNotNull(removed);
            assertEquals(1, removed.properties().count().next().intValue());
            assertEquals(new Long(0), removed.bothE().count().next());
            assertTrue(removed.property("deleted").isPresent());
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldNotCallBaseFunctionThusNotRemovingTheEdge() throws Exception {
            final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);

            // create an ad-hoc strategy that only marks a vertex as "deleted" and removes all edges and properties
            // but doesn't actually blow it away
            swg.strategy().setGraphStrategy(new GraphStrategy() {
                @Override
                public UnaryOperator<Supplier<Void>> getRemoveEdgeStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
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

            swg.e(e.id()).remove();

            final Edge removed = g.e(e.id());
            assertNotNull(removed);
            assertEquals(1, removed.properties().count().next().intValue());
            assertTrue(removed.property("deleted").isPresent());
        }

        @Test
        public void shouldAdHocTheCloseStrategy() throws Exception {
            final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);

            final AtomicInteger counter = new AtomicInteger(0);
            swg.strategy().setGraphStrategy(new GraphStrategy() {
                @Override
                public UnaryOperator<Supplier<Void>> getGraphCloseStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
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

        @Parameterized.Parameters(name = "{index}: {0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, Edge, Stream<Property<Object>>>>> tests = new ArrayList<>();
            tests.add(Pair.with("e.property(\"all\")", (Graph g, Edge e) -> Stream.of(e.property("all"))));
            tests.add(Pair.with("e.iterators().properties()", (Graph g, Edge e) -> StreamFactory.stream(e.iterators().properties())));
            tests.add(Pair.with("e.iterators().properties(\"any\")", (Graph g, Edge e) -> StreamFactory.stream(e.iterators().properties("any"))));
            tests.add(Pair.with("e.iterators().hiddens()", (Graph g, Edge e) -> StreamFactory.stream(e.iterators().hiddens())));
            tests.add(Pair.with("e.iterators().hiddens(\"hideme\")", (Graph g, Edge e) -> StreamFactory.stream(e.iterators().hiddens("hideme"))));
            tests.add(Pair.with("e.properties()", (Graph g, Edge e) -> StreamFactory.stream(e.properties())));
            tests.add(Pair.with("e.property(\"extra\",\"more\")", (Graph g, Edge e) -> Stream.<Property<Object>>of(e.property("extra", "more"))));
            //tests.add(Pair.with("g.E().properties(\"all\")", (Graph g, Edge e) -> g.E().properties("all").toList().stream()));
            //tests.add(Pair.with("g.E().properties()", (Graph g, Edge e) -> g.E().properties().toList().stream()));

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
            final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
            swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
            final Vertex v = swg.addVertex();
            final Edge e = v.addEdge("to", v, "all", "a", "any", "something", Graph.Key.hide("hideme"), "hidden");

            final AtomicBoolean atLeastOne = new AtomicBoolean(false);
            assertTrue(streamGetter.apply(swg, e).allMatch(p -> {
                atLeastOne.set(true);
                return p instanceof StrategyWrappedProperty;
            }));

            assertTrue(atLeastOne.get());
        }
    }

    @RunWith(Parameterized.class)
    public static class VertexPropertyPropertyShouldBeWrappedTests extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "{index}: {0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, VertexProperty, Stream<Property<Object>>>>> tests = new ArrayList<>();
            tests.add(Pair.with("vp.property(\"food\")", (Graph g, VertexProperty vp) -> Stream.of(vp.property("food"))));
            tests.add(Pair.with("vp.property(\"moreFood\",\"sandwhich\")", (Graph g, VertexProperty vp) -> Stream.of(vp.property("moreFood","sandwhich"))));
            tests.add(Pair.with("vp.property(Graph.Key.hide(\"more\"))", (Graph g, VertexProperty vp) -> Stream.of(vp.property(Graph.Key.hide("more")))));
            tests.add(Pair.with("vp.property(Graph.Key.hide(\"extra\",\"this\"))", (Graph g, VertexProperty vp) -> Stream.of(vp.property(Graph.Key.hide("extra"), "this"))));
            tests.add(Pair.with("vp.iterators().properties()", (Graph g, VertexProperty vp) -> StreamFactory.stream(vp.iterators().properties())));
            tests.add(Pair.with("vp.iterators().properties(\"food\")", (Graph g, VertexProperty vp) -> StreamFactory.stream(vp.iterators().properties("food"))));
            tests.add(Pair.with("vp.iterators().hiddens(\"more\")", (Graph g, VertexProperty vp) -> StreamFactory.stream(vp.iterators().hiddens("more"))));
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
            final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
            swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
            final Vertex v = swg.addVertex();
            final VertexProperty vp = v.property("property", "on-property", "food", "taco", Graph.Key.hide("food"), "burger", "more", "properties", Graph.Key.hide("more"), "hidden");

            final AtomicBoolean atLeastOne = new AtomicBoolean(false);
            assertTrue(streamGetter.apply(swg, vp).allMatch(p -> {
                atLeastOne.set(true);
                return p instanceof StrategyWrappedProperty;
            }));

            assertTrue(atLeastOne.get());
        }
    }

    @RunWith(Parameterized.class)
    public static class VertexPropertyShouldBeWrappedTest extends AbstractGremlinTest {
        @Parameterized.Parameters(name = "{index}: {0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, Vertex, Stream<VertexProperty<Object>>>>> tests = new ArrayList<>();
            tests.add(Pair.with("v.property(\"all\")", (Graph g, Vertex v) -> Stream.of(v.property("all"))));
            tests.add(Pair.with("v.property(\"extra\", \"data\")", (Graph g, Vertex v) -> Stream.of(v.<Object>property("extra", "data"))));
            tests.add(Pair.with("v.iterators().properties()", (Graph g, Vertex v) -> StreamFactory.stream(v.iterators().properties())));
            tests.add(Pair.with("v.iterators().properties(\"any\")", (Graph g, Vertex v) -> StreamFactory.stream(v.iterators().properties("any"))));
            tests.add(Pair.with("v.iterators().hiddens()", (Graph g, Vertex v) -> StreamFactory.stream(v.iterators().hiddens())));
            tests.add(Pair.with("v.iterators().hiddens(\"hideme\")", (Graph g, Vertex v) -> StreamFactory.stream(v.iterators().hiddens("hideme"))));
            tests.add(Pair.with("v.properties()", (Graph g, Vertex v) -> StreamFactory.stream(v.properties())));
            tests.add(Pair.with("v.property(\"extra\",\"more\")", (Graph g, Vertex v) -> Stream.<VertexProperty<Object>>of(v.property("extra", "more"))));
            // todo: test e.iterators().vertices() with "crew"

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
        public void shouldWrapProperty() {
            final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
            swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
            final Vertex v = swg.addVertex("all", "a", "any", "something", Graph.Key.hide("hideme"), "hidden");

            final AtomicBoolean atLeastOne = new AtomicBoolean(false);
            assertTrue(streamGetter.apply(g, v).allMatch(p -> {
                atLeastOne.set(true);
                return p instanceof StrategyWrappedVertexProperty;
            }));

            assertTrue(atLeastOne.get());
        }
    }

    @RunWith(Parameterized.class)
    public static class VertexPropertyWithMultiPropertyShouldBeWrappedTest extends AbstractGremlinTest {
        @Parameterized.Parameters(name = "{index}: {0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, Vertex, Stream<VertexProperty<Object>>>>> tests = new ArrayList<>();
            tests.add(Pair.with("v.property(\"all\")", (Graph g, Vertex v) -> Stream.of(v.property("all"))));
            tests.add(Pair.with("v.property(\"extra\", \"data\")", (Graph g, Vertex v) -> Stream.of(v.<Object>property("extra", "data"))));
            tests.add(Pair.with("v.iterators().properties()", (Graph g, Vertex v) -> StreamFactory.stream(v.iterators().properties())));
            tests.add(Pair.with("v.iterators().properties(\"any\")", (Graph g, Vertex v) -> StreamFactory.stream(v.iterators().properties("any"))));
            tests.add(Pair.with("v.iterators().hiddens()", (Graph g, Vertex v) -> StreamFactory.stream(v.iterators().hiddens())));
            tests.add(Pair.with("v.iterators().hiddens(\"hideme\")", (Graph g, Vertex v) -> StreamFactory.stream(v.iterators().hiddens("hideme"))));
            tests.add(Pair.with("v.properties()", (Graph g, Vertex v) -> StreamFactory.stream(v.properties())));
            tests.add(Pair.with("v.property(\"extra\",\"more\")", (Graph g, Vertex v) -> Stream.<VertexProperty<Object>>of(v.property("extra", "more"))));

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
        public void shouldWrapProperty() {
            final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
            swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
            final Vertex v = swg.addVertex("all", "a", "any", "something", "any", "something-else", Graph.Key.hide("hideme"), "hidden", Graph.Key.hide("hideme"), "hidden-too");

            final AtomicBoolean atLeastOne = new AtomicBoolean(false);
            assertTrue(streamGetter.apply(g, v).allMatch(p -> {
                atLeastOne.set(true);
                return p instanceof StrategyWrappedVertexProperty;
            }));

            assertTrue(atLeastOne.get());
        }
    }

    @RunWith(Parameterized.class)
    public static class EdgeShouldBeWrappedTest extends AbstractGremlinTest {
        @Parameterized.Parameters(name = "{index}: {0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, AbstractGremlinTest, Stream<Edge>>>> tests = new ArrayList<>();
            tests.add(Pair.with("g.E()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.E())));
            tests.add(Pair.with("g.e(11)", (Graph g, AbstractGremlinTest instance) -> Stream.of(g.e(instance.convertToEdgeId("josh", "created", "lop")))));
            tests.add(Pair.with("g.v(1).outE()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("marko")).outE())));
            tests.add(Pair.with("g.v(4).outE()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("josh")).bothE())));
            tests.add(Pair.with("g.v(4).outE()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("josh")).inE())));
            tests.add(Pair.with("g.v(1).iterators().edge(Direction.BOTH, 1)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("marko")).iterators().edges(Direction.BOTH, 1))));
            tests.add(Pair.with("g.v(4).iterators().edge(Direction.BOTH, Integer.MAX_VALUE)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("josh")).iterators().edges(Direction.BOTH, Integer.MAX_VALUE))));
            tests.add(Pair.with("g.v(1).iterators().edge(Direction.BOTH, Integer.MAX_VALUE)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("marko")).iterators().edges(Direction.OUT, Integer.MAX_VALUE))));
            tests.add(Pair.with("g.v(4).iterators().edge(Direction.BOTH, Integer.MAX_VALUE)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("josh")).iterators().edges(Direction.IN, Integer.MAX_VALUE))));

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
            final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
            swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);

            final AtomicBoolean atLeastOne = new AtomicBoolean(false);
            assertTrue(streamGetter.apply(swg, this).allMatch(e -> {
                atLeastOne.set(true);
                return e instanceof StrategyWrappedEdge;
            }));

            assertTrue(atLeastOne.get());
        }
    }

    @RunWith(Parameterized.class)
    public static class VertexShouldBeWrappedTest extends AbstractGremlinTest {
        @Parameterized.Parameters(name = "{index}: {0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, BiFunction<Graph, AbstractGremlinTest, Stream<Vertex>>>> tests = new ArrayList<>();
            tests.add(Pair.with("g.V()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.V())));
            tests.add(Pair.with("g.v(1)", (Graph g, AbstractGremlinTest instance) -> Stream.of(g.v(instance.convertToVertexId("marko")))));
            tests.add(Pair.with("g.v(1).outE().inV()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("marko")).outE().inV())));
            tests.add(Pair.with("g.v(4).bothE().bothV()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("josh")).bothE().bothV())));
            tests.add(Pair.with("g.v(4).inE().outV()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("josh")).inE().outV())));
            tests.add(Pair.with("g.v(1).out()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("marko")).out())));
            tests.add(Pair.with("g.v(1).iterators().vertices(Direction.BOTH, 1)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("marko")).iterators().vertices(Direction.BOTH, 1))));
            tests.add(Pair.with("g.v(4).iterators().vertices(Direction.BOTH, Integer.MAX_VALUE)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("josh")).iterators().vertices(Direction.BOTH, Integer.MAX_VALUE))));
            tests.add(Pair.with("g.v(1).iterators().vertices(Direction.BOTH, Integer.MAX_VALUE)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("marko")).iterators().vertices(Direction.OUT, Integer.MAX_VALUE))));
            tests.add(Pair.with("g.v(4).iterators().vertices(Direction.BOTH, Integer.MAX_VALUE)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("josh")).iterators().vertices(Direction.IN, Integer.MAX_VALUE))));
            tests.add(Pair.with("g.v(4).iterators().vertices(Direction.BOTH, Integer.MAX_VALUE, \"created\")", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("josh")).iterators().vertices(Direction.BOTH, Integer.MAX_VALUE, "created"))));
            tests.add(Pair.with("g.e(11).iterators().vertices(Direction.IN)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.e(instance.convertToEdgeId("josh", "created", "lop")).iterators().vertices(Direction.IN))));
            tests.add(Pair.with("g.e(11).iterators().vertices(Direction.OUT)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.e(instance.convertToEdgeId("josh", "created", "lop")).iterators().vertices(Direction.OUT))));
            tests.add(Pair.with("g.e(11).iterators().vertices(Direction.BOTH)", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.e(instance.convertToEdgeId("josh", "created", "lop")).iterators().vertices(Direction.BOTH))));
            tests.add(Pair.with("g.v(1).iterators().properties(\"name\").next().getElement()", (Graph g, AbstractGremlinTest instance) -> StreamFactory.stream(g.v(instance.convertToVertexId("marko")).iterators().properties("name").next().getElement())));

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
            final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
            swg.strategy.setGraphStrategy(GraphStrategy.DoNothingGraphStrategy.INSTANCE);

            final AtomicBoolean atLeastOne = new AtomicBoolean(false);
            assertTrue(streamGetter.apply(swg, this).allMatch(v -> {
                atLeastOne.set(true);
                return v instanceof StrategyWrappedVertex;
            }));

            assertTrue(atLeastOne.get());
        }
    }
}
