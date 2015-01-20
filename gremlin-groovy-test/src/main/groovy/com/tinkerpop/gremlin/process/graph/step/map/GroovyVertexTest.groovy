package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Compare
import com.tinkerpop.gremlin.structure.Direction
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyVertexTest {

    public static class StandardTest extends VertexTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V() {
            g.V
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out(final Object v1Id) {
            g.V(v1Id).out
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v2_in(final Object v2Id) {
            g.V(v2Id).in
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_both(final Object v4Id) {
            g.V(v4Id).both
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_localXoutEXknowsX_limitX1XX_inV_name(final Object v1Id) {
            g.V(v1Id).local(__.outE('knows').limit(1)).inV.name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXbothEXcreatedX_limitX1XX_otherV_name() {
            g.V().local(__.bothE('created').limit(1)).otherV.name
        }

        @Override
        public Traversal<Edge, Edge> get_g_E() {
            g.E
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outE(final Object v1Id) {
            g.V(v1Id).outE
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v2_inE(final Object v2Id) {
            g.V(v2Id).inE
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothE(final Object v4Id) {
            g.V(v4Id).bothE
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEXcreatedX(final Object v4Id) {
            g.V(v4Id).bothE('created')
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_localXbothEX1_createdX_limitX1XX(final Object v4Id) {
            g.V(v4Id).local(__.bothE('created').limit(1))
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_localXbothEXknows_createdX_limitX1XX(final Object v4Id) {
            g.V(v4Id).local(__.bothE('knows', 'created').limit(1))
        }

        @Override
        public Traversal<Vertex, String> get_g_v4_localXbothE_limitX1XX_otherV_name(final Object v4Id) {
            g.V(v4Id).local(__.bothE.limit(1)).otherV.name
        }

        @Override
        public Traversal<Vertex, String> get_g_v4_localXbothE_limitX2XX_otherV_name(final Object v4Id) {
            g.V(v4Id).local(__.bothE.limit(2).otherV).name
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXinEXknowsX_limitX2XX_outV_name() {
            g.V().inE('knows').local(__.limit(2).outV).name
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outE_inV(final Object v1Id) {
            g.V(v1Id).outE.inV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v2_inE_outV(final Object v2Id) {
            g.V(v2Id).inE.outV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_outE_hasXweight_1X_outV() {
            g.V.outE.has('weight', 1.0d).outV
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_outE_inV_inE_inV_both_name() {
            g.V.out.outE.inV.inE.inV.both.name
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_outEXknowsX_bothV_name(final Object v1Id) {
            g.V(v1Id).outE('knows').bothV.name
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknowsX(final Object v1Id) {
            g.V(v1Id).out('knows')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknows_createdX(final Object v1Id) {
            g.V(v1Id).out('knows', 'created')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outEXknowsX_inV(final Object v1Id) {
            g.V(v1Id).outE('knows').inV()
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outEXknows_createdX_inV(final Object v1Id) {
            g.V(v1Id).outE('knows', 'created').inV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out() {
            g.V().out().out()
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_out_out(final Object v1Id) {
            g.V(v1Id).out.out.out
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_out_name(final Object v1Id) {
            g.V(v1Id).out.name
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outE_otherV(final Object v1Id) {
            g.V(v1Id).outE.otherV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothE_otherV(final Object v4Id) {
            g.V(v4Id).bothE.otherV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothE_hasXweight_lt_1X_otherV(final Object v4Id) {
            g.V(v4Id).bothE.has('weight', Compare.lt, 1.0d).otherV
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_to_XOUT_knowsX(final Object v1Id) {
            g.V(v1Id).to(Direction.OUT, 'knows');
        }
    }

    public static class ComputerTest extends VertexTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V() {
            ComputerTestHelper.compute("g.V", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v2_in(final Object v2Id) {
            ComputerTestHelper.compute("g.V(${v2Id}).in", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_both(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).both", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_localXoutEXknowsX_limitX1XX_inV_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).local(__.outE('knows').limit(1)).inV.name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXbothEXcreatedX_limitX1XX_otherV_name() {
            ComputerTestHelper.compute("g.V().local(__.bothE('created').limit(1)).otherV.name", g);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E() {
            ComputerTestHelper.compute("g.E", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v1_outE(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v2_inE(final Object v2Id) {
            ComputerTestHelper.compute("g.V(${v2Id}).inE", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothE(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).bothE", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEXcreatedX(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).bothE('created')", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_localXbothEX1_createdX_limitX1XX(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).local(__.bothE('created').limit(1))", g);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_localXbothEXknows_createdX_limitX1XX(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).local(__.bothE('knows', 'created').limit(1))", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_v4_localXbothE_limitX1XX_otherV_name(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).local(__.bothE.limit(1)).otherV.name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_v4_localXbothE_limitX2XX_otherV_name(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).local(__.bothE.limit(2)).otherV.name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_localXinEXknowsX_limitX2XX_outV_name() {
            ComputerTestHelper.compute("g.V().local(__.inE('knows').limit(2).outV).name", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outE_inV(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE.inV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v2_inE_outV(final Object v2Id) {
            ComputerTestHelper.compute("g.V(${v2Id}).inE.outV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_outE_hasXweight_1X_outV() {
            ComputerTestHelper.compute("g.V.outE.has('weight', 1.0d).outV", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_outE_inV_inE_inV_both_name() {
            ComputerTestHelper.compute("g.V.out.outE.inV.inE.inV.both.name", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_outEXknowsX_bothV_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE('knows').bothV.name", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknowsX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out('knows')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outXknows_createdX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out('knows', 'created')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outEXknowsX_inV(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE('knows').inV()", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outEXknows_createdX_inV(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE('knows', 'created').inV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out() {
            ComputerTestHelper.compute("g.V().out().out()", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_out_out_out(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out.out.out", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_out_name(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out.name", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_outE_otherV(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).outE.otherV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothE_otherV(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).bothE.otherV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothE_hasXweight_lt_1X_otherV(final Object v4Id) {
            ComputerTestHelper.compute("g.V(${v4Id}).bothE.has('weight', Compare.lt, 1.0d).otherV", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v1_to_XOUT_knowsX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).to(Direction.OUT, 'knows')", g);
        }
    }
}
