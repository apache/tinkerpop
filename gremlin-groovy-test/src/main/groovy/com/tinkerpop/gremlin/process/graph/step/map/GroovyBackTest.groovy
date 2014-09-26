package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyBackTest {

    public static class StandardTest extends BackTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_asXhereX_out_backXhereX(final Object v1Id) {
            g.v(v1Id).as('here').out.back('here')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX(final Object v4Id) {
            g.v(v4Id).out.as('here').has('lang', 'java').back('here')
        }

        @Override
        public Traversal<Vertex, String> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX(
                final Object v4Id) {
            g.v(v4Id).out.as('here').has('lang', 'java').back('here').value('name')
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX(final Object v1Id) {
            g.v(v1Id).outE.as('here').inV.has('name', 'vadas').back('here')
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(
                final Object v1Id) {
            g.v(v1Id).outE('knows').has('weight', 1.0d).as('here').inV.has('name', 'josh').back('here')
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_backXhereX(
                final Object v1Id) {
            g.v(v1Id).outE('knows').as('here').has('weight', 1.0d).inV.has('name', 'josh').back('here')
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_backXhereX(
                final Object v1Id) {
            g.v(v1Id).outE("knows").as('here').has('weight', 1.0d).as('fake').inV.has("name", 'josh').back('here')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXhereXout_valueXnameX_backXhereX() {
            g.V().as("here").out.value("name").back("here");
        }
    }

    public static class ComputerTest extends BackTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_asXhereX_out_backXhereX(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).as('here').out.back('here')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX(final Object v4Id) {
            ComputerTestHelper.compute("g.v(${v4Id}).out.as('here').has('lang', 'java').back('here')", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX_valueXnameX(
                final Object v4Id) {
            ComputerTestHelper.compute("g.v(${v4Id}).out.as('here').has('lang', 'java').back('here').value('name')", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outE_asXhereX_inV_hasXname_vadasX_backXhereX(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).outE.as('here').inV.has('name', 'vadas').back('here')", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(
                final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).outE('knows').has('weight', 1.0d).as('here').inV.has('name', 'josh').back('here')", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_backXhereX(
                final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).outE('knows').as('here').has('weight', 1.0d).inV.has('name','josh').back('here')", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_backXhereX(
                final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).outE('knows').as('here').has('weight', 1.0d).as('fake').inV.has('name','josh').back('here')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_asXhereXout_valueXnameX_backXhereX() {
            ComputerTestHelper.compute("g.V().as('here').out.value('name').back('here')", g);
        }
    }
}
