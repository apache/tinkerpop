package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SequenceGraphStrategyTest extends AbstractBlueprintsTest {
    @Test
    public void shouldAppendPropertyValuesInOrderToVertex() {
        g.strategy().set(Optional.of(new SequenceGraphStrategy(
            new GraphStrategy() {
                @Override
                public UnaryOperator<Function<Object[], Vertex>> getWrapAddVertex() {
                    return (f) -> (args) -> {
                        final List<Object> o = new ArrayList<>(Arrays.asList(args));
                        o.addAll(Arrays.asList("anonymous", "working1"));
                        return f.apply(o.toArray());
                    };
                }
            },
            new GraphStrategy() {
                @Override
                public UnaryOperator<Function<Object[], Vertex>> getWrapAddVertex() {
                    return (f) -> (args) -> {
                        final List<Object> o = new ArrayList<>(Arrays.asList(args));
                        o.addAll(Arrays.asList("anonymous", "working2"));
                        o.addAll(Arrays.asList("try","anything"));
                        return f.apply(o.toArray());
                    };
                }
            },
            new GraphStrategy() {
                @Override
                public UnaryOperator<Function<Object[], Vertex>> getWrapAddVertex() {
                    return (f) -> (args) -> {
                        final List<Object> o = new ArrayList<>(Arrays.asList(args));
                        o.addAll(Arrays.asList("anonymous", "working3"));
                        return f.apply(o.toArray());
                    };
                }
            })));

        final Vertex v = g.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.getProperty("any").getValue());
        assertEquals("working3", v.getProperty("anonymous").getValue());
        assertEquals("anything", v.getProperty("try").getValue());
    }

    @Test(expected = RuntimeException.class)
    public void shouldShortCircuitStrategyWithException() {
        g.strategy().set(Optional.of(new SequenceGraphStrategy(
            new GraphStrategy() {
                @Override
                public UnaryOperator<Function<Object[], Vertex>> getWrapAddVertex() {
                    return (f) -> (args) -> {
                        final List<Object> o = new ArrayList<>(Arrays.asList(args));
                        o.addAll(Arrays.asList("anonymous", "working1"));
                        return f.apply(o.toArray());
                    };
                }
            },
            new GraphStrategy() {
                @Override
                public UnaryOperator<Function<Object[], Vertex>> getWrapAddVertex() {
                    return (f) -> (args) -> {
                        throw new RuntimeException("test");
                    };
                }
            },
            new GraphStrategy() {
                @Override
                public UnaryOperator<Function<Object[], Vertex>> getWrapAddVertex() {
                    return (f) -> (args) -> {
                        final List<Object> o = new ArrayList<>(Arrays.asList(args));
                        o.addAll(Arrays.asList("anonymous", "working3"));
                        return f.apply(o.toArray());
                    };
                }
            })));

        g.addVertex("any", "thing");
    }

    @Test
    public void shouldShortCircuitStrategyWithNoOp() {
        g.strategy().set(Optional.of(new SequenceGraphStrategy(
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getWrapAddVertex() {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working1"));
                            return f.apply(o.toArray());
                        };
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getWrapAddVertex() {
                        // if "f" is not applied then the next step and following steps won't process
                        return (f) -> (args) -> null;
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getWrapAddVertex() {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working3"));
                            return f.apply(o.toArray());
                        };
                    }
                })));

        assertNull(g.addVertex("any", "thing"));
    }

    @Test
    public void shouldDoSomethingBeforeAndAfter() {
        g.strategy().set(Optional.of(new SequenceGraphStrategy(
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getWrapAddVertex() {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working1"));
                            return f.apply(o.toArray());
                        };
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getWrapAddVertex() {
                        return (f) -> (args) -> {
                            final Vertex v = f.apply(args);

                            // this  means that the next strategy and those below it executed including
                            // the implementation
                            assertEquals("working3", v.getProperty("anonymous").getValue());

                            // now do something with that vertex after the fact
                            v.setProperty("anonymous", "working2");

                            return v;
                        };
                    }
                },
                new GraphStrategy() {
                    @Override
                    public UnaryOperator<Function<Object[], Vertex>> getWrapAddVertex() {
                        return (f) -> (args) -> {
                            final List<Object> o = new ArrayList<>(Arrays.asList(args));
                            o.addAll(Arrays.asList("anonymous", "working3"));
                            return f.apply(o.toArray());
                        };
                    }
                })));

        final Vertex v = g.addVertex("any", "thing");

        assertNotNull(v);
        assertEquals("thing", v.getProperty("any").getValue());
        assertEquals("working2", v.getProperty("anonymous").getValue());
    }
}
