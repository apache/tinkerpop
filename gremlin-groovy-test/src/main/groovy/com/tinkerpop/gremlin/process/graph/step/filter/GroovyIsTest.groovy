package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Compare
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class GroovyIsTest {

    public static class StandardTest extends IsTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isX32X() {
            return g.V().values('age').is(32);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXlte_30X() {
            return g.V().values('age').is(Compare.lte, 30);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXgte_29X_isXlt_34X() {
            return g.V().values('age').is(Compare.gte, 29).is(Compare.lt, 34);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX() {
            return g.V().has(__.in('created').count().is(1l)).values('name');
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isXgte_2XX_valuesXnameX() {
            return g.V().has(__.in('created').count().is(Compare.gte, 2l)).values('name');
        }
    }

    public static class ComputerTest extends IsTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isX32X() {
            ComputerTestHelper.compute("g.V().values('age').is(32)", g)
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXlte_30X() {
            ComputerTestHelper.compute("g.V().values('age').is(Compare.lte, 30)", g)
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXgte_29X_isXlt_34X() {
            ComputerTestHelper.compute("g.V().values('age').is(Compare.gte, 29).is(Compare.lt, 34)", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX() {
            ComputerTestHelper.compute("g.V().has(__.in('created').count().is(1l)).values('name')", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isXgte_2XX_valuesXnameX() {
            ComputerTestHelper.compute("g.V().has(__.in('created').count().is(Compare.gte, 2l)).values('name')", g)
        }
    }
}
