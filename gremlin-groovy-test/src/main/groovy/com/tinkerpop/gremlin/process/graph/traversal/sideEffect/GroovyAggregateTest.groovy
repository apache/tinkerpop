package com.tinkerpop.gremlin.process.graph.traversal.sideEffect

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.AggregateTest
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyAggregateTest {

    public static class StandardTest extends AggregateTest {

        @Override
        public Traversal<Vertex, List<String>> get_g_V_name_aggregate() {
            g.V.name.aggregate
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_aggregate_byXnameX() {
            g.V.aggregate.by('name')
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_aggregateXaX_path() {
            g.V.out.aggregate('a').path;
        }
    }

    public static class ComputerTest extends AggregateTest {

        @Override
        public Traversal<Vertex, List<String>> get_g_V_name_aggregate() {
            ComputerTestHelper.compute("g.V.name.aggregate", g)
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_aggregate_byXnameX() {
            ComputerTestHelper.compute("g.V.aggregate.by('name')", g)
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_aggregateXaX_path() {
            ComputerTestHelper.compute("g.V.out.aggregate('a').path", g)
        }
    }
}
