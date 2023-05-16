/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class HasTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_hasXkeyX(final Object v1Id, final String key);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_hasXname_markoX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_markoX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_blahX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXblahX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXnullX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXnull_testnullkeyX();

    public abstract Traversal<Edge, Edge> get_g_E_hasXnullX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasLabelXnullX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasLabelXnull_personX();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_hasXage_gt_30X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VXv1X_hasXage_gt_30X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_hasXid_lt_3X(final Object v1Id, final Object v3Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2X(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2_3X(final Object v1Id, final Object v2Id, final Object v3Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2_3X_inList(final Object v1Id, final Object v2Id, final Object v3Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasIdX2_3X(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasIdX2_3X_inList(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXage_isXgt_30XX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXlabel_isXsoftwareXX();

    public abstract Traversal<Edge, Edge> get_g_EX7X_hasLabelXknowsX(final Object e7Id);

    public abstract Traversal<Edge, Edge> get_g_E_hasLabelXknowsX();

    public abstract Traversal<Edge, Edge> get_g_E_hasLabelXnullX();

    public abstract Traversal<Vertex, ? extends Property<Object>> get_g_V_properties_hasLabelXnullX();

    public abstract Traversal<Edge, Edge> get_g_EX11X_outV_outE_hasXid_10X(final Object e11Id, final Object e8Id);

    public abstract Traversal<Edge, Edge> get_g_E_hasLabelXuses_traversesX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasLabelXperson_software_blahX();

    public abstract Traversal<Vertex, Integer> get_g_V_hasXperson_name_markoX_age();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outE_hasXweight_inside_0_06X_inV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXlocationX();

    public abstract Traversal<Vertex, Vertex> get_g_V_in_hasIdXneqX1XX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_V_hasLabelXpersonX_hasXage_notXlteX10X_andXnotXbetweenX11_20XXXX_andXltX29X_orXeqX35XXXX_name();

    public abstract Traversal<Vertex, Integer> get_g_V_both_properties_dedup_hasKeyXageX_value();

    public abstract Traversal<Vertex, Integer> get_g_V_both_properties_dedup_hasKeyXageX_hasValueXgtX30XX_value();

    public abstract Traversal<Vertex, Double> get_g_V_bothE_properties_dedup_hasKeyXweightX_value();

    public abstract Traversal<Vertex, Double> get_g_V_bothE_properties_dedup_hasKeyXweightX_hasValueXltX0d3XX_value();

    public abstract Traversal<Vertex, String> get_g_V_hasNotXageX_name();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasIdX1X_hasIdX2X(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasLabelXpersonX_hasLabelXsoftwareX();

    public abstract Traversal<Vertex, Long> get_g_V_hasIdXemptyX_count();

    public abstract Traversal<Vertex, Long> get_g_V_hasIdXwithinXemptyXX_count();

    public abstract Traversal<Vertex, Long> get_g_V_hasIdXwithoutXemptyXX_count();

    public abstract Traversal<Vertex, Long> get_g_V_notXhasIdXwithinXemptyXXX_count();

    public abstract Traversal<Vertex, Long> get_g_V_hasXage_withinX27X_count();

    public abstract Traversal<Vertex, Long> get_g_V_hasXage_withinX27_29X_count();

    public abstract Traversal<Vertex, Long> get_g_V_hasXage_withoutX27X_count();

    public abstract Traversal<Vertex, Long> get_g_V_hasXage_withoutX27_29X_count();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_containingXarkXX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_startingWithXmarXX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_endingWithXasXX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_not_containingXarkXX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_not_startingWithXmarXX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_not_endingWithXasXX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_regexXrMarXX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_notRegexXrMarXX();

    public abstract Traversal<Vertex, String> get_g_V_hasXname_regexXTinkerXX();

    public abstract Traversal<Vertex, String> get_g_V_hasXname_regexXTinkerUnicodeXX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXperson_name_containingXoX_andXltXmXXX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_gtXmX_andXcontainingXoXXX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXp_neqXvXX();

    public abstract Traversal<Vertex, String> get_g_V_hasXk_withinXcXX_valuesXkX();

    public abstract Traversal<Vertex, ? extends Property<Object>> get_g_V_properties_hasKeyXnullX();

    public abstract Traversal<Vertex, ? extends Property<Object>> get_g_V_properties_hasKeyXnull_nullX();

    public abstract Traversal<Vertex, Integer> get_g_V_properties_hasKeyXnull_ageX_value();

    public abstract Traversal<Edge, ? extends Property<Object>> get_g_E_properties_hasKeyXnullX();

    public abstract Traversal<Edge, ? extends Property<Object>> get_g_E_properties_hasKeyXnull_nullX();

    public abstract Traversal<Edge, Double> get_g_E_properties_hasKeyXnull_weightX_value();

    public abstract Traversal<Vertex, ? extends Property<Object>> get_g_V_properties_hasValueXnullX();

    public abstract Traversal<Vertex, ? extends Property<Object>> get_g_V_properties_hasValueXnull_nullX();

    public abstract Traversal<Vertex, String> get_g_V_properties_hasValueXnull_joshX_value();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("ripple"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_hasXnameX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_hasXkeyX(convertToVertexId("marko"), "name");
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_hasXcircumferenceX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_hasXkeyX(convertToVertexId("marko"), "circumference");
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_hasXname_markoX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_hasXname_markoX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX2X_hasXname_markoX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_hasXname_markoX(convertToVertexId("vadas"));
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_markoX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_markoX();
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_blahX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_blahX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXage_gt_30X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXage_gt_30X();
        printTraversalForm(traversal);
        final List<Vertex> list = traversal.toList();
        assertEquals(2, list.size());
        for (final Element v : list) {
            assertTrue(v.<Integer>value("age") > 30);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXage_isXgt_30XX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXage_isXgt_30XX();
        printTraversalForm(traversal);
        final List<Vertex> list = traversal.toList();
        assertEquals(2, list.size());
        for (final Element v : list) {
            assertTrue(v.<Integer>value("age") > 30);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXlabel_isXsoftwareXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXlabel_isXsoftwareXX();
        printTraversalForm(traversal);
        final List<Vertex> list = traversal.toList();
        assertEquals(2, list.size());
        for (final Element v : list) {
            assertEquals("software", v.label());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_hasXage_gt_30X() {
        final Traversal<Vertex, Vertex> traversalMarko = get_g_VX1X_hasXage_gt_30X(convertToVertexId("marko"));
        printTraversalForm(traversalMarko);
        assertFalse(traversalMarko.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX4X_hasXage_gt_30X() {
        final Traversal<Vertex, Vertex> traversalJosh = get_g_VX1X_hasXage_gt_30X(convertToVertexId("josh"));
        printTraversalForm(traversalJosh);
        assertTrue(traversalJosh.hasNext());
        traversalJosh.iterate();
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VXv1X_hasXage_gt_30X() {
        final Traversal<Vertex, Vertex> traversalMarko = get_g_VXv1X_hasXage_gt_30X(convertToVertex(graph,"marko"));
        printTraversalForm(traversalMarko);
        assertFalse(traversalMarko.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VXv4X_hasXage_gt_30X() {
        final Traversal<Vertex, Vertex> traversalJosh = get_g_VXv1X_hasXage_gt_30X(convertToVertex(graph,"josh"));
        printTraversalForm(traversalJosh);
        assertTrue(traversalJosh.hasNext());
        traversalJosh.iterate();
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_hasXid_2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_hasIdX2X(convertToVertexId("marko"), convertToVertexId("vadas"));
        assertVadasAsOnlyValueReturned(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS)
    public void g_VX1X_out_hasXid_lt_3X() {
        // can only execute this on graphs with user supplied ids so that we can be assured of the lt op. it
        // sort of assumes that ids increment, but there's no feature check for that.  graphs that don't work this
        // way with numeric ids may need to optout
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_hasXid_lt_3X(convertToVertexId("marko"), convertToVertexId("lop"));
        assertVadasAsOnlyValueReturned(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1AsStringX_out_hasXid_2AsStringX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_hasIdX2X(convertToVertexId("marko").toString(), convertToVertexId("vadas").toString());
        assertVadasAsOnlyValueReturned(traversal);
    }

    private void assertVadasAsOnlyValueReturned(final Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(true));
        assertEquals(convertToVertexId("vadas"), traversal.next().id());
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_hasXid_2_3X() {
        final Object id2 = convertToVertexId("vadas");
        final Object id3 = convertToVertexId("lop");
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_hasIdX2_3X(convertToVertexId("marko"), id2, id3);
        assert_g_has_2id(id2, id3, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_hasXid_2AsString_3AsStringX() {
        final Object id2 = convertToVertexId("vadas");
        final Object id3 = convertToVertexId("lop");
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_hasIdX2_3X(convertToVertexId("marko"), id2.toString(), id3.toString());
        assert_g_has_2id(id2, id3, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_hasXid_2_3X_inList() {
        final Object id2 = convertToVertexId("vadas");
        final Object id3 = convertToVertexId("lop");
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_hasIdX2_3X_inList(convertToVertexId("marko"), id2, id3);
        assert_g_has_2id(id2, id3, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXid_1_2X() {
        final Object id1 = convertToVertexId("marko");
        final Object id2 = convertToVertexId("vadas");
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasIdX2_3X(id1, id2);
        assert_g_has_2id(id1, id2, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXid_1_2X_inList() {
        final Object id1 = convertToVertexId("marko");
        final Object id2 = convertToVertexId("vadas");
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasIdX2_3X_inList(id1, id2);
        assert_g_has_2id(id1, id2, traversal);
    }

    // asserts that the traversal returns two ids
    protected void assert_g_has_2id(Object id1, Object id2, Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertThat(traversal.next().id(), CoreMatchers.anyOf(is(id1), is(id2)));
        assertThat(traversal.next().id(), CoreMatchers.anyOf(is(id1), is(id2)));
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXblahX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXblahX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXnullX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXnullX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXnull_testnullkeyX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXnull_testnullkeyX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_hasXnullX() {
        final Traversal<Edge, Edge> traversal = get_g_E_hasXnullX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXnullX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasLabelXnullX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXnull_personX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasLabelXnull_personX();
        printTraversalForm(traversal);
        final List<Vertex> vertices = traversal.toList();
        assertEquals(4, vertices.size());
        assertThat(vertices.stream().allMatch(v -> v.label().equals("person")), is(true));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_EX7X_hasLabelXknowsX() {
        final Traversal<Edge, Edge> traversal = get_g_EX7X_hasLabelXknowsX(convertToEdgeId("marko", "knows", "vadas"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals("knows", traversal.next().label());
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_hasLabelXknowsX() {
        final Traversal<Edge, Edge> traversal = get_g_E_hasLabelXknowsX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals("knows", traversal.next().label());
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_hasLabelXnullX() {
        final Traversal<Edge, Edge> traversal = get_g_E_hasLabelXnullX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_properties_hasLabelXnullX() {
        final Traversal<Vertex, ? extends Property> traversal = get_g_V_properties_hasLabelXnullX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_E_hasLabelXuses_traversesX() {
        final Traversal<Edge, Edge> traversal = get_g_E_hasLabelXuses_traversesX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String label = traversal.next().label();
            assertTrue(label.equals("uses") || label.equals("traverses"));
        }
        assertEquals(9, counter);
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_V_hasLabelXperson_software_blahX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasLabelXperson_software_blahX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String label = traversal.next().label();
            assertTrue(label.equals("software") || label.equals("person"));
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXperson_name_markoX_age() {
        final Traversal<Vertex, Integer> traversal = get_g_V_hasXperson_name_markoX_age();
        printTraversalForm(traversal);
        assertEquals(29, traversal.next().intValue());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outE_hasXweight_inside_0_06X_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outE_hasXweight_inside_0_06X_inV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        while (traversal.hasNext()) {
            Vertex vertex = traversal.next();
            assertTrue(vertex.value("name").equals("vadas") || vertex.value("name").equals("lop"));
        }
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_EX11X_outV_outE_hasXid_10X() {
        final Object edgeId11 = convertToEdgeId("josh", "created", "lop");
        final Object edgeId10 = convertToEdgeId("josh", "created", "ripple");
        final Traversal<Edge, Edge> traversal = get_g_EX11X_outV_outE_hasXid_10X(edgeId11, edgeId10);
        printTraversalForm(traversal);
        assert_g_EX11X(edgeId10, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_EX11X_outV_outE_hasXid_10AsStringX() {
        final Object edgeId11 = convertToEdgeId("josh", "created", "lop");
        final Object edgeId10 = convertToEdgeId("josh", "created", "ripple");
        final Traversal<Edge, Edge> traversal = get_g_EX11X_outV_outE_hasXid_10X(edgeId11.toString(), edgeId10.toString());
        printTraversalForm(traversal);
        assert_g_EX11X(edgeId10, traversal);
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_V_hasXlocationX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXlocationX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(convertToVertex(graph, "marko"), convertToVertex(graph, "stephen"), convertToVertex(graph, "daniel"), convertToVertex(graph, "matthias")), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_hasXage_notXlteX10X_andXnotXbetweenX11_20XXXX_andXltX29X_orXeqX35XXXX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_hasLabelXpersonX_hasXage_notXlteX10X_andXnotXbetweenX11_20XXXX_andXltX29X_orXeqX35XXXX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("peter", "vadas"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_in_hasIdXneqX1XX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_in_hasIdXneqX1XX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            Vertex vertex = traversal.next();
            assertTrue(vertex.value("name").equals("josh") || vertex.value("name").equals("peter"));
            count++;
        }
        assertEquals(3, count);
    }

    private void assert_g_EX11X(final Object edgeId, final Traversal<Edge, Edge> traversal) {
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Edge e = traversal.next();
        assertEquals(edgeId, e.id());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_dedup_properties_hasKeyXageX_value() {
        final Traversal<Vertex, Integer> traversal = get_g_V_both_properties_dedup_hasKeyXageX_value();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(29, 27, 32, 35), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_properties_dedup_hasKeyXageX_hasValueXgtX30XX_value() {
        final Traversal<Vertex, Integer> traversal = get_g_V_both_properties_dedup_hasKeyXageX_hasValueXgtX30XX_value();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(32, 35), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_bothE_properties_dedup_hasKeyXweightX_value() {
        final Traversal<Vertex, Double> traversal = get_g_V_bothE_properties_dedup_hasKeyXweightX_value();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(0.5, 1.0, 0.4, 0.2), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_bothE_properties_dedup_hasKeyXweightX_hasValueXltX0d3XX_value() {
        final Traversal<Vertex, Double> traversal = get_g_V_bothE_properties_dedup_hasKeyXweightX_hasValueXltX0d3XX_value();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(0.2), traversal);
    }

    @Test
    @LoadGraphWith
    public void g_V_hasNotXageX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_hasNotXageX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("lop", "ripple"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasIdX1X_hasIdX2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasIdX1X_hasIdX2X(
                convertToVertexId("marko"), convertToVertexId("vadas")
        );
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_hasLabelXsoftwareX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasLabelXpersonX_hasLabelXsoftwareX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasIdXemptyX_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_hasIdXemptyX_count();
        printTraversalForm(traversal);
        assertEquals(0L, traversal.next().longValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasIdXwithinXemptyXX_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_hasIdXwithinXemptyXX_count();
        printTraversalForm(traversal);
        assertEquals(0L, traversal.next().longValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasIdXwithoutXemptyXX_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_hasIdXwithoutXemptyXX_count();
        printTraversalForm(traversal);
        assertEquals(6L, traversal.next().longValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_notXhasIdXwithinXemptyXXX_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_notXhasIdXwithinXemptyXXX_count();
        printTraversalForm(traversal);
        assertEquals(6L, traversal.next().longValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXage_withinX27X_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_hasXage_withinX27X_count();
        printTraversalForm(traversal);
        assertEquals(1L, traversal.next().longValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXage_withinX27_29X_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_hasXage_withinX27_29X_count();
        printTraversalForm(traversal);
        assertEquals(2L, traversal.next().longValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXage_withoutX27X_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_hasXage_withoutX27X_count();
        printTraversalForm(traversal);
        assertEquals(3L, traversal.next().longValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXage_withoutX27_29X_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_hasXage_withoutX27_29X_count();
        printTraversalForm(traversal);
        assertEquals(2L, traversal.next().longValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_containingXarkXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_containingXarkXX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertTrue(traversal.next().value("name").equals("marko"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_startingWithXmarXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_startingWithXmarXX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertTrue(traversal.next().value("name").equals("marko"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_endingWithXasXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_endingWithXasXX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertTrue(traversal.next().value("name").equals("vadas"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_not_containingXarkXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_not_containingXarkXX();
        printTraversalForm(traversal);

        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertNotEquals("marko", traversal.next().value("name"));
        }
        assertEquals(5, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_not_startingWithXmarXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_not_startingWithXmarXX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertNotEquals("marko", traversal.next().value("name"));
        }
        assertEquals(5, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_not_endingWithXasXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_not_endingWithXasXX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertNotEquals("vadas", traversal.next().value("name"));
        }
        assertEquals(5, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_regexXrMarXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_regexXrMarXX();
        printTraversalForm(traversal);

        assertTrue(traversal.hasNext());
        assertTrue(traversal.next().value("name").equals("marko"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_notRegexXrMarXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_notRegexXrMarXX();
        printTraversalForm(traversal);

        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertNotEquals("marko", traversal.next().value("name"));
        }
        assertEquals(5, counter);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_V_hasXname_regexXTinkerXX() {
        g.addV("software").property("name", "Apache TinkerPop©").iterate();

        final Traversal<Vertex, String> traversal = get_g_V_hasXname_regexXTinkerXX();
        printTraversalForm(traversal);

        assertTrue(traversal.hasNext());
        assertTrue(traversal.next().equals("Apache TinkerPop©"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_V_hasXname_regexXTinkerUnicodeXX() {
        g.addV("software").property("name", "Apache TinkerPop©").iterate();

        final Traversal<Vertex, String> traversal = get_g_V_hasXname_regexXTinkerUnicodeXX();
        printTraversalForm(traversal);

        assertTrue(traversal.hasNext());
        assertTrue(traversal.next().equals("Apache TinkerPop©"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXperson_name_containingXoX_andXltXmXXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXperson_name_containingXoX_andXltXmXXX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertTrue(traversal.next().value("name").equals("josh"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_gtXmX_andXcontainingXoXXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_gtXmX_andXcontainingXoXXX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertTrue(traversal.next().value("name").equals("marko"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXp_neqXvXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXp_neqXvXX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_V_hasXk_withinXcXX_valuesXkX() {
        g.addV().property("k", "轉注").
                addV().property("k", "✦").
                addV().property("k", "♠").
                addV().property("k", "A").iterate();

        final Traversal<Vertex, String> traversal = get_g_V_hasXk_withinXcXX_valuesXkX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("轉注", "✦", "♠"), traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_properties_hasKeyXnullX() {
        final Traversal<Vertex, ? extends Property<Object>> traversal = get_g_V_properties_hasKeyXnullX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_properties_hasKeyXnull_nullX() {
        final Traversal<Vertex, ? extends Property<Object>> traversal = get_g_V_properties_hasKeyXnull_nullX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_properties_hasKeyXnull_ageX_value() {
        final Traversal<Vertex, Integer> traversal = get_g_V_properties_hasKeyXnull_ageX_value();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(27, 29, 32, 35), traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_properties_hasKeyXnullX() {
        final Traversal<Edge, ? extends Property<Object>> traversal = get_g_E_properties_hasKeyXnullX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_properties_hasKeyXnull_nullX() {
        final Traversal<Edge, ? extends Property<Object>> traversal = get_g_E_properties_hasKeyXnull_nullX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_properties_hasKeyXnull_weightX_value() {
        final Traversal<Edge, Double> traversal = get_g_E_properties_hasKeyXnull_weightX_value();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(0.5, 1.0, 1.0, 0.4, 0.4, 0.2), traversal);
        assertThat(traversal.hasNext(), is(false));
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_properties_hasValueXnullX() {
        final Traversal<Vertex, ? extends Property<Object>> traversal = get_g_V_properties_hasValueXnullX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_properties_hasValueXnull_nullX() {
        final Traversal<Vertex, ? extends Property<Object>> traversal = get_g_V_properties_hasValueXnull_nullX();
        printTraversalForm(traversal);
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_properties_hasValueXnull_joshX_value() {
        final Traversal<Vertex, String> traversal = get_g_V_properties_hasValueXnull_joshX_value();
        printTraversalForm(traversal);
        assertEquals("josh", traversal.next());
        assertThat(traversal.hasNext(), is(false));
    }

    public static class Traversals extends HasTest {
        @Override
        public Traversal<Edge, Edge> get_g_EX11X_outV_outE_hasXid_10X(final Object e11Id, final Object e10Id) {
            return g.E(e11Id).outV().outE().has(T.id, e10Id);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name() {
            return g.V().out("created").has("name", __.<String, Integer>map(s -> s.get().length()).is(P.gt(3))).values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXkeyX(final Object v1Id, final String key) {
            return g.V(v1Id).has(key);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXname_markoX(final Object v1Id) {
            return g.V(v1Id).has("name", "marko");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX() {
            return g.V().has("name", "marko");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_blahX() {
            return g.V().has("name", "blah");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXblahX() {
            return g.V().has("blah");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXnullX() {
            return g.V().has(null);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXnull_testnullkeyX() {
            return g.V().has((String) null, "test-null-key");
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasXnullX() {
            return g.E().has(null);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasLabelXnullX() {
            return g.V().hasLabel(null);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasLabelXnull_personX() {
            return g.V().hasLabel(null, "person");
        }

        @Override
        public Traversal<Vertex, ? extends Property<Object>> get_g_V_properties_hasLabelXnullX() {
            return g.V().properties().hasLabel(null);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXage_gt_30X(final Object v1Id) {
            return g.V(v1Id).has("age", P.gt(30));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VXv1X_hasXage_gt_30X(final Object v1Id) {
            return g.V(v1Id).has("age", P.gt(30));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasXid_lt_3X(final Object v1Id, final Object v3Id) {
            return g.V(v1Id).out().has(T.id, P.lt(v3Id));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2X(final Object v1Id, final Object v2Id) {
            return g.V(v1Id).out().hasId(v2Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2_3X(final Object v1Id, final Object v2Id, final Object v3Id) {
            return g.V(v1Id).out().hasId(v2Id, v3Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2_3X_inList(final Object v1Id, final Object v2Id, final Object v3Id) {
            return g.V(v1Id).out().hasId(Arrays.asList(v2Id, v3Id));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasIdX2_3X(final Object v1Id, final Object v2Id) {
            return g.V().hasId(v1Id, v2Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasIdX2_3X_inList(final Object v1Id, final Object v2Id) {
            return g.V().hasId(Arrays.asList(v1Id, v2Id));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X() {
            return g.V().has("age", P.gt(30));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXage_isXgt_30XX() {
            return g.V().has("age", __.is(P.gt(30)));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXlabel_isXsoftwareXX() {
            return g.V().has(T.label, __.is("software"));
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX7X_hasLabelXknowsX(final Object e7Id) {
            return g.E(e7Id).hasLabel("knows");
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXknowsX() {
            return g.E().hasLabel("knows");
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXnullX() {
            return g.E().hasLabel(null);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXuses_traversesX() {
            return g.E().hasLabel("uses", "traverses");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasLabelXperson_software_blahX() {
            return g.V().hasLabel("person", "software", "blah");
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_hasXperson_name_markoX_age() {
            return g.V().has("person", "name", "marko").values("age");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_hasXweight_inside_0_06X_inV(final Object v1Id) {
            return g.V(v1Id).outE().has("weight", P.inside(0.0d, 0.6d)).inV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXlocationX() {
            return g.V().has("location");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_in_hasIdXneqX1XX(final Object v1Id) {
            return g.V().in().hasId(P.neq(v1Id));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasLabelXpersonX_hasXage_notXlteX10X_andXnotXbetweenX11_20XXXX_andXltX29X_orXeqX35XXXX_name() {
            return g.V().hasLabel("person").has("age", P.not(P.lte(10).and(P.not(P.between(11, 20)))).and(P.lt(29).or(P.eq(35)))).values("name");
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_both_properties_dedup_hasKeyXageX_value() {
            return g.V().both().properties().dedup().hasKey("age").value();
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_both_properties_dedup_hasKeyXageX_hasValueXgtX30XX_value() {
            return g.V().both().properties().dedup().hasKey("age").hasValue(P.gt(30)).value();
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_bothE_properties_dedup_hasKeyXweightX_value() {
            return g.V().bothE().properties().dedup().hasKey("weight").value();
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_bothE_properties_dedup_hasKeyXweightX_hasValueXltX0d3XX_value() {
            return g.V().bothE().properties().dedup().hasKey("weight").hasValue(P.lt(0.3)).value();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasNotXageX_name() {
            return g.V().hasNot("age").values("name");
        }

        @Override
        public Traversal<Vertex, Vertex>  get_g_V_hasIdX1X_hasIdX2X(final Object v1Id, final Object v2Id) {
            return g.V().hasId(v1Id).hasId(v2Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasLabelXpersonX_hasLabelXsoftwareX() {
            return g.V().hasLabel("person").hasLabel("software");
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_hasIdXemptyX_count() {
            return g.V().hasId(Collections.emptyList()).count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_hasIdXwithinXemptyXX_count() {
            return g.V().hasId(P.within(Collections.emptyList())).count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_hasIdXwithoutXemptyXX_count() {
            return g.V().hasId(P.without(Collections.emptyList())).count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_notXhasIdXwithinXemptyXXX_count() {
            return g.V().not(__.hasId(P.within(Collections.emptyList()))).count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_hasXage_withinX27X_count() {
            return g.V().has("age", P.within(27)).count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_hasXage_withinX27_29X_count() {
            return g.V().has("age", P.within(27, 29)).count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_hasXage_withoutX27X_count() {
            return g.V().has("age", P.without(27)).count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_hasXage_withoutX27_29X_count() {
            return g.V().has("age", P.without(27, 29)).count();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_containingXarkXX() {
            return g.V().has("name", TextP.containing("ark"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_startingWithXmarXX() {
            return g.V().has("name", TextP.startingWith("mar"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_endingWithXasXX() {
            return g.V().has("name", TextP.endingWith("as"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_not_containingXarkXX() {
            return g.V().has("name", TextP.notContaining("ark"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_not_startingWithXmarXX() {
            return g.V().has("name", TextP.notStartingWith("mar"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_not_endingWithXasXX() {
            return g.V().has("name", TextP.notEndingWith("as"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXperson_name_containingXoX_andXltXmXXX() {
            return g.V().has("person","name", TextP.containing("o").and(P.lt("m")));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_gtXmX_andXcontainingXoXXX() {
            return g.V().has("name", P.gt("m").and(TextP.containing("o")));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXp_neqXvXX() {
            return g.V().has("p", P.neq("v"));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXk_withinXcXX_valuesXkX() {
            return g.V().has("k", P.within("轉注", "✦", "♠")).values("k");
        }

        @Override
        public Traversal<Vertex, ? extends Property<Object>> get_g_V_properties_hasKeyXnullX() {
            return g.V().properties().hasKey(null);
        }

        @Override
        public Traversal<Vertex, ? extends Property<Object>> get_g_V_properties_hasKeyXnull_nullX() {
            return g.V().properties().hasKey(null,null);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_properties_hasKeyXnull_ageX_value() {
            return g.V().properties().hasKey(null, "age").value();
        }

        @Override
        public Traversal<Edge, ? extends Property<Object>> get_g_E_properties_hasKeyXnullX() {
            return g.E().properties().hasKey(null);
        }

        @Override
        public Traversal<Edge, ? extends Property<Object>> get_g_E_properties_hasKeyXnull_nullX() {
            return g.E().properties().hasKey(null,null);
        }

        @Override
        public Traversal<Edge, Double> get_g_E_properties_hasKeyXnull_weightX_value() {
            return g.E().properties().hasKey(null, "weight").value();
        }

        @Override
        public Traversal<Vertex, ? extends Property<Object>> get_g_V_properties_hasValueXnullX() {
            return g.V().properties().hasValue(null);
        }

        @Override
        public Traversal<Vertex, ? extends Property<Object>> get_g_V_properties_hasValueXnull_nullX() {
            return g.V().properties().hasValue(null,null);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_properties_hasValueXnull_joshX_value() {
            return g.V().properties().hasValue(null, "josh").value();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_regexXrMarXX() {
            return g.V().has("name", TextP.regex("^mar"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_notRegexXrMarXX() {
            return g.V().has("name", TextP.notRegex("^mar"));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXname_regexXTinkerXX() {
            return g.V().has("name", TextP.regex("Tinker")).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXname_regexXTinkerUnicodeXX() {
            return g.V().has("name", TextP.regex("Tinker.*\u00A9")).values("name");
        }
    }
}