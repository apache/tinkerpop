package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.util.function.TriFunction;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SequenceGraphStrategyTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    public void shouldAppendMultiPropertyValuesToVertex() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(new SequenceGraphStrategy(
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working1"));
                            return f.apply(o.toArray());
                        };
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working2"));
                            o.addAll(Arrays.asList("try", "anything"));
                            return f.apply(o.toArray());
                        };
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return UnaryOperator.identity();
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working3"));
                            return f.apply(o.toArray());
                        };
                    }
                }
        ));

        final Vertex v = swg.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.property("any").value());
        assertEquals(3, v.values("anonymous").toList().size());
        assertTrue(v.values("anonymous").toList().contains("working1"));
        assertTrue(v.values("anonymous").toList().contains("working2"));
        assertTrue(v.values("anonymous").toList().contains("working3"));
        assertEquals("anything", v.property("try").value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldOverwritePropertyValuesToVertex() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(new SequenceGraphStrategy(
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working1"));
                            return f.apply(o.toArray());
                        };
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.remove("anonymous");
                            o.remove("working1");
                            o.addAll(Arrays.asList("anonymous", "working2"));
                            o.addAll(Arrays.asList("try", "anything"));
                            return f.apply(o.toArray());
                        };
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return UnaryOperator.identity();
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.remove("anonymous");
                            o.remove("working2");
                            o.addAll(Arrays.asList("anonymous", "working3"));
                            return f.apply(o.toArray());
                        };
                    }
                }
        ));

        final Vertex v = swg.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.property("any").value());
        assertEquals(1, v.values("anonymous").toList().size());
        assertTrue(v.values("anonymous").toList().contains("working3"));
        assertEquals("anything", v.property("try").value());
    }


    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldAlterArgumentsToAddVertexInOrderOfSequence() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(new SequenceGraphStrategy(
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working1"));
                            return f.apply(o.toArray());
                        };
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.replaceAll(it -> it.equals("working1") ? "working2" : it);
                            o.addAll(Arrays.asList("try", "anything"));
                            return f.apply(o.toArray());
                        };
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return UnaryOperator.identity();
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.replaceAll(it -> it.equals("working2") ? "working3" : it);
                            return f.apply(o.toArray());
                        };
                    }
                }
        ));

        final Vertex v = swg.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.property("any").value());
        assertEquals("working3", v.property("anonymous").value());
        assertEquals("anything", v.property("try").value());
    }

    @Test(expected = RuntimeException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldShortCircuitStrategyWithException() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(new SequenceGraphStrategy(
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working1"));
                            return f.apply(o.toArray());
                        };
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            throw new RuntimeException("test");
                        };
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working3"));
                            return f.apply(o.toArray());
                        };
                    }
                }
        ));

        swg.addVertex("any", "thing");
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldShortCircuitStrategyWithNoOp() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(new SequenceGraphStrategy(
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working1"));
                            return f.apply(o.toArray());
                        };
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        // if "f" is not applied then the next step and following steps won't process
                        return (f) -> (args) -> null;
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working3"));
                            return f.apply(o.toArray());
                        };
                    }
                }
        ));

        assertNull(swg.addVertex("any", "thing"));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldDoSomethingBeforeAndAfter() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(new SequenceGraphStrategy(
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final Vertex v = f.apply(args);
                            // this  means that the next strategy and those below it executed including
                            // the implementation
                            assertEquals("working2", v.property("anonymous").value());
                            // now do something with that vertex after the fact
                            v.properties("anonymous").remove();
                            v.property("anonymous", "working1");
                            return v;
                        };

                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final Vertex v = f.apply(args);
                            // this  means that the next strategy and those below it executed including
                            // the implementation
                            assertEquals("working3", v.property("anonymous").value());
                            // now do something with that vertex after the fact
                            v.properties("anonymous").remove();
                            v.property("anonymous", "working2");
                            return v;
                        };
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working3"));
                            return f.apply(o.toArray());
                        };
                    }
                }
        ));

        final Vertex v = swg.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.property("any").value());
        assertEquals("working1", v.property("anonymous").value());
    }

    @Test
    public void shouldHaveAllMethodsImplemented() throws Exception {
        final Method[] methods = GraphStrategy.class.getDeclaredMethods();
        final SpyGraphStrategy spy = new SpyGraphStrategy();
        final SequenceGraphStrategy strategy = new SequenceGraphStrategy(spy);

        // invoke all the strategy methods
        Stream.of(methods).forEach(method -> {
            try {
                if (method.getName().equals("applyStrategyToTraversal"))
                    method.invoke(strategy, new DefaultGraphTraversal<>());
                else
                    method.invoke(strategy, new Strategy.Context(g, new StrategyWrapped() {
                    }));

            } catch (Exception ex) {
                ex.printStackTrace();
                fail("Should be able to invoke function");
                throw new RuntimeException("fail");
            }
        });

        // check the spy to see that all methods were executed
        assertEquals(methods.length, spy.getCount());
    }

    public class SpyGraphStrategy implements GraphStrategy {

        private int count = 0;

        public int getCount() {
            return count;
        }

        private UnaryOperator spy() {
            count++;
            return UnaryOperator.identity();
        }

        @Override
        public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Void>> getRemoveEdgeStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Supplier<Void>> getRemovePropertyStrategy(final Strategy.Context<StrategyWrappedProperty<V>> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Function<String, VertexProperty<V>>> getVertexGetPropertyStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Function<String, Property<V>>> getEdgeGetPropertyStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<BiFunction<String, V, VertexProperty<V>>> getVertexPropertyStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<BiFunction<String, V, Property<V>>> getEdgePropertyStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Object>> getVertexIdStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Graph>> getVertexGraphStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Object>> getEdgeIdStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Graph>> getEdgeGraphStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<String>> getVertexLabelStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<String>> getEdgeLabelStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Set<String>>> getVertexKeysStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Set<String>>> getEdgeKeysStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Set<String>>> getVertexHiddenKeysStrategy(Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Set<String>>> getEdgeHiddenKeysStrategy(Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Function<Object, Vertex>> getGraphvStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Function<Object, Edge>> getGrapheStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Function<String, V>> getVertexValueStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Function<String, V>> getEdgeValueStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Set<String>>> getVariableKeysStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
            return spy();
        }

        @Override
        public <R> UnaryOperator<Function<String, Optional<R>>> getVariableGetStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<BiConsumer<String, Object>> getVariableSetStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Consumer<String>> getVariableRemoveStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Map<String, Object>>> getVariableAsMapStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Void>> getGraphCloseStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
            return spy();
        }

        @Override
        public GraphTraversal applyStrategyToTraversal(final GraphTraversal traversal) {
            spy();
            return traversal;
        }

        @Override
        public <V> UnaryOperator<Function<String[], Iterator<VertexProperty<V>>>> getVertexIteratorsPropertiesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Function<String[], Iterator<Property<V>>>> getEdgeIteratorsPropertiesStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Function<String[], Iterator<VertexProperty<V>>>> getVertexIteratorsHiddensStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Function<String[], Iterator<? extends Property<V>>>> getEdgeIteratorsHiddensStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Function<String[], Iterator<V>>> getVertexIteratorsValuesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Function<String[], Iterator<V>>> getEdgeIteratorsValuesStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Function<String[], Iterator<V>>> getVertexIteratorsHiddenValuesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Function<String[], Iterator<V>>> getEdgeIteratorsHiddenValuesStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<TriFunction<Direction, Integer, String[], Iterator<Vertex>>> getVertexIteratorsVerticesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<TriFunction<Direction, Integer, String[], Iterator<Edge>>> getVertexIteratorsEdgesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Supplier<Void>> getRemoveVertexPropertyStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Function<Direction, Iterator<Vertex>>> getEdgeIteratorsVerticesStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Supplier<Object>> getVertexPropertyIdStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Supplier<String>> getVertexPropertyLabelStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Supplier<Graph>> getVertexPropertyGraphStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
            return spy();
        }


        @Override
        public <V> UnaryOperator<Supplier<Set<String>>> getVertexPropertyKeysStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Supplier<Set<String>>> getVertexPropertyHiddenKeysStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Supplier<Vertex>> getVertexPropertyGetElementStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
            return spy();
        }

        @Override
        public <V, U> UnaryOperator<BiFunction<String, V, Property<V>>> getVertexPropertyPropertyStrategy(final Strategy.Context<StrategyWrappedVertexProperty<U>> ctx) {
            return spy();
        }

        @Override
        public <V, U> UnaryOperator<Function<String[], Iterator<Property<V>>>> getVertexPropertyIteratorsPropertiesStrategy(final Strategy.Context<StrategyWrappedVertexProperty<U>> ctx) {
            return spy();
        }

        @Override
        public <V, U> UnaryOperator<Function<String[], Iterator<? extends Property<V>>>> getVertexPropertyIteratorsHiddensStrategy(final Strategy.Context<StrategyWrappedVertexProperty<U>> ctx) {
            return spy();
        }

        @Override
        public <V, U> UnaryOperator<Function<String[], Iterator<V>>> getVertexPropertyIteratorsValuesStrategy(final Strategy.Context<StrategyWrappedVertexProperty<U>> ctx) {
            return spy();
        }

        @Override
        public <V, U> UnaryOperator<Function<String[], Iterator<V>>> getVertexPropertyIteratorsHiddenValuesStrategy(final Strategy.Context<StrategyWrappedVertexProperty<U>> ctx) {
            return spy();
        }
    }
}
