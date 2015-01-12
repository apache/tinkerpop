package com.tinkerpop.gremlin.process.graph.step.filter

import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Compare
import com.tinkerpop.gremlin.structure.Contains
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyHasTest {

    public static class StandardTest extends HasTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXkeyX(final Object v1Id, final String key) {
            g.V(v1Id).has(key)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXname_markoX(final Object v1Id) {
            g.V(v1Id).has('name', 'marko')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX() {
            g.V.has('name', 'marko')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_blahX() {
            g.V.has('name', 'blah')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXblahX() {
            g.V.has('blah')
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXage_gt_30X(final Object v1Id) {
            g.V(v1Id).has('age', Compare.gt, 30)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasXid_2X(final Object v1Id, final Object v2Id) {
            g.V(v1Id).out.has(T.id, v2Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X() {
            g.V.has('age', Compare.gt, 30)
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX7X_hasXlabelXknowsX(final Object e7Id) {
            g.E(e7Id).has(T.label, 'knows')
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasXlabelXknowsX() {
            g.E.has(T.label, 'knows')
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasXlabelXuses_traversesX() {
            g.E.has(T.label, Contains.within, ['uses', 'traverses'])
        }

        @Override
        Traversal<Vertex, Vertex> get_g_V_hasXlabelXperson_software_blahX() {
            g.V.has(T.label, Contains.within, ["person", "software", 'blah']);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_equalspredicate_markoX() {
            g.V().has("name", { v1, v2 -> v1.equals(v2) }, "marko");
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_hasXperson_name_markoX_age() {
            g.V.has('person', 'name', 'marko').age;
        }
    }

    public static class ComputerTest extends HasTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXkeyX(final Object v1Id, final String key) {
            ComputerTestHelper.compute("g.V(${v1Id}).has('${key}')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXname_markoX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).has('name', 'marko')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX() {
            ComputerTestHelper.compute("g.V.has('name', 'marko')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_blahX() {
            ComputerTestHelper.compute(" g.V.has('name', 'blah')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXblahX() {
            ComputerTestHelper.compute("g.V.has('blah')", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXage_gt_30X(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).has('age', Compare.gt, 30)", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasXid_2X(final Object v1Id, final Object v2Id) {
            ComputerTestHelper.compute(" g.V(${v1Id}).out.has(T.id, ${v2Id})", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X() {
            ComputerTestHelper.compute("g.V.has('age', Compare.gt, 30)", g);
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX7X_hasXlabelXknowsX(final Object e7Id) {
            ComputerTestHelper.compute(" g.E(${e7Id}).has(T.label, 'knows')", g);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasXlabelXknowsX() {
            ComputerTestHelper.compute("g.E.has(T.label, 'knows')", g);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasXlabelXuses_traversesX() {
            ComputerTestHelper.compute("g.E.has(T.label, Contains.within, ['uses', 'traverses'])", g);
        }

        @Override
        Traversal<Vertex, Vertex> get_g_V_hasXlabelXperson_software_blahX() {
            ComputerTestHelper.compute("g.V.has(T.label, Contains.within, ['person', 'software', 'blah'])", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_equalspredicate_markoX() {
            ComputerTestHelper.compute(" g.V().has('name', { v1, v2 -> v1.equals(v2) }, 'marko')", g);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_hasXperson_name_markoX_age() {
            ComputerTestHelper.compute("g.V.has('person', 'name', 'marko').age", g);
        }
    }
}
