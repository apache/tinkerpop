package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.groovy.function.GFunction
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySelectTest {

    public static class StandardTest extends SelectTest {

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_v1_asXaX_outXknowsX_asXbX_select(final Object v1Id) {
            g.V(v1Id).as('a').out('knows').as('b').select()
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX(final Object v1Id) {
            g.V(v1Id).as('a').out('knows').as('b').select.by('name')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id) {
            g.V(v1Id).as('a').out('knows').as('b').select('a')
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX(
                final Object v1Id) {
            g.V(v1Id).as('a').out('knows').as('b').select('a').by('name')
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_selectXnameX() {
            g.V.as('a').out.as('b').select.by('name')
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_aggregate_asXbX_selectXnameX() {
            g.V.as('a').out.aggregate.as('b').select.by('name')
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_name_order_asXbX_selectXname_itX() {
            g.V().as('a').name.order().as('b').select.by('name', new GFunction({ it }));
        }
    }

    public static class ComputerTest extends SelectTest {

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_v1_asXaX_outXknowsX_asXbX_select(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).as('a').out('knows').as('b').select()", g);
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_v1_asXaX_outXknowsX_asXbX_selectXnameX(final Object v1Id) {
            g.V(v1Id).as('a').out('knows').as('b').select.by('name')
            // TODO: ComputerTestHelper.compute("g.V(${v1Id}).as('a').out('knows').as('b').select { it.value('name') }",g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_asXaX_outXknowsX_asXbX_selectXaX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).as('a').out('knows').as('b').select('a')", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX(
                final Object v1Id) {
            g.V(v1Id).as('a').out('knows').as('b').select('a').by('name')
            //TODO: ComputerTestHelper.compute("g.V(${v1Id}).as('a').out('knows').as('b').select(['a']) { it.name }",g);
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_selectXnameX() {
            g.V.as('a').out.as('b').select.by('name') // TODO computer
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_aggregate_asXbX_selectXnameX() {
            g.V.as('a').out.aggregate.as('b').select.by('name') // TODO computer
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_name_order_asXbX_selectXname_itX() {
            g.V().as('a').name.order().as('b').select.by('name', new GFunction({ it })) // TODO: computer
        }
    }
}
