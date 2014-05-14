package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.TriFunction;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SequenceGraphStrategyTest extends AbstractGremlinTest {
    @Test
    public void shouldAppendPropertyValuesInOrderToVertex() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(Optional.of(new SequenceGraphStrategy(
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
        )));

        final Vertex v = swg.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.getProperty("any").get());
        assertEquals("working3", v.getProperty("anonymous").get());
        assertEquals("anything", v.getProperty("try").get());
    }

    @Test(expected = RuntimeException.class)
    public void shouldShortCircuitStrategyWithException() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(Optional.of(new SequenceGraphStrategy(
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
        )));

        swg.addVertex("any", "thing");
    }

    @Test
    public void shouldShortCircuitStrategyWithNoOp() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(Optional.of(new SequenceGraphStrategy(
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
        )));

        assertNull(swg.addVertex("any", "thing"));
    }

    @Test
    public void shouldDoSomethingBeforeAndAfter() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(Optional.of(new SequenceGraphStrategy(
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
                            final Vertex v = f.apply(args);

                            // this  means that the next strategy and those below it executed including
                            // the implementation
                            assertEquals("working3", v.getProperty("anonymous").get());

                            // now do something with that vertex after the fact
                            v.setProperty("anonymous", "working2");

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
        )));

        final Vertex v = swg.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.getProperty("any").get());
        assertEquals("working2", v.getProperty("anonymous").get());
    }

    @Test
    public void shouldHaveAllMethodsImplemented() throws Exception {
        final Method[] methods = GraphStrategy.class.getDeclaredMethods();
        final SpyGraphStrategy spy = new SpyGraphStrategy();
        final SequenceGraphStrategy strategy = new SequenceGraphStrategy(spy);

        // invoke all the strategy methods
        Stream.of(methods).forEach(method -> {
            try {
                method.invoke(strategy, new Strategy.Context(g, new StrategyWrapped() {}));
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
        public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(Strategy.Context<StrategyWrappedGraph> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(Strategy.Context<StrategyWrappedVertex> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Void>> getRemoveElementStrategy(Strategy.Context<? extends StrategyWrappedElement> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Supplier<Void>> getRemovePropertyStrategy(Strategy.Context<StrategyWrappedProperty<V>> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<Function<String, Property<V>>> getElementGetProperty(Strategy.Context<? extends StrategyWrappedElement> ctx) {
            return spy();
        }

        @Override
        public <V> UnaryOperator<BiFunction<String, V, Property<V>>> getElementSetProperty(Strategy.Context<? extends StrategyWrappedElement> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<Object>> getElementGetId(Strategy.Context<? extends StrategyWrappedElement> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Function<Object, Vertex>> getGraphvStrategy(Strategy.Context<StrategyWrappedGraph> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Function<Object, Edge>> getGrapheStrategy(Strategy.Context<StrategyWrappedGraph> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<GraphTraversal<Vertex, Vertex>>> getVStrategy(Strategy.Context<StrategyWrappedGraph> ctx) {
            return spy();
        }

        @Override
        public UnaryOperator<Supplier<GraphTraversal<Edge, Edge>>> getEStrategy(Strategy.Context<StrategyWrappedGraph> ctx) {
            return spy();
        }
    }
}
