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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.ExceptionCoverage;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.process.T;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@ExceptionCoverage(exceptionClass = Vertex.Exceptions.class, methods = {
        "multiplePropertiesExistForProvidedKey",
})
@RunWith(Enclosed.class)
public class VertexPropertyTest extends AbstractGremlinTest {

    public static class VertexPropertyAddition extends AbstractGremlinTest {

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_USER_SUPPLIED_IDS)
        public void shouldAllowIdAssignment() {
            final Vertex v = g.addVertex();
            final Object id = graphProvider.convertId(123131231l);
            v.property("name", "stephen", T.id, id);

            tryCommit(g, g -> assertEquals(id, v.property("name").id()));
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldSetLabelOfVertexPropertyToKeyOfOwningProperty() {
            final Vertex v = g.addVertex("name", "stephen");
            tryCommit(g, g -> assertEquals("name", v.property("name").label()));
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES)
        public void shouldHandleListVertexProperties() {
            final Vertex v = g.addVertex("name", "marko", "age", 34);
            tryCommit(g, g -> {
                assertEquals("marko", v.property("name").value());
                assertEquals("marko", v.value("name"));
                assertEquals(34, v.property("age").value());
                assertEquals(34, v.<Integer>value("age").intValue());
                assertEquals(1, IteratorUtils.count(v.properties("name")));
                assertEquals(2, IteratorUtils.count(v.properties()));
                assertVertexEdgeCounts(1, 0);
            });

            final VertexProperty<String> property = v.property("name", "marko a. rodriguez");
            tryCommit(g, g -> assertEquals(v, property.element()));

            try {
                v.property("name");
                fail("This should throw a: " + Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name"));
            } catch (final Exception e) {
                validateException(Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name"), e);
            }

            assertTrue(IteratorUtils.collectMap(v.properties(), Property::key, p -> IteratorUtils.list(p.values())).get("name").contains("marko"));
            assertTrue(IteratorUtils.collectMap(v.properties(), Property::key, p -> IteratorUtils.list(p.values())).get("name").contains("marko a. rodriguez"));
            assertEquals(3, IteratorUtils.count(v.properties()));
            assertEquals(2, IteratorUtils.count(v.properties("name")));
            assertVertexEdgeCounts(1, 0);

            assertEquals(v, v.property("name", "mrodriguez").element());
            tryCommit(g, g -> {
                assertEquals(3, IteratorUtils.count(v.properties("name")));
                assertEquals(4, IteratorUtils.count(v.properties()));
                assertVertexEdgeCounts(1, 0);
            });

            v.<String>properties("name").forEachRemaining(meta -> {
                meta.property("counter", meta.value().length());
            });

            tryCommit(g, g -> {
                v.properties().forEachRemaining(meta -> {
                    assertEquals(meta.key(), meta.label());
                    assertTrue(meta.isPresent());
                    assertEquals(v, meta.element());
                    if (meta.key().equals("age")) {
                        assertEquals(meta.value(), 34);
                        assertEquals(0, IteratorUtils.count(meta.properties()));
                    }
                    if (meta.key().equals("name")) {
                        assertEquals(((String) meta.value()).length(), meta.<Integer>value("counter").intValue());
                        assertEquals(1, IteratorUtils.count(meta.properties()));
                        assertEquals(1, meta.keys().size());
                        assertTrue(meta.keys().contains("counter"));
                    }
                });

                assertVertexEdgeCounts(1, 0);
            });
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
        public void shouldHandleSingleVertexProperties() {
            final Vertex v = g.addVertex("name", "marko", "name", "marko a. rodriguez", "name", "marko rodriguez");
            tryCommit(g, g -> {
                assertEquals(3, IteratorUtils.count(v.properties()));
                assertEquals(3, IteratorUtils.count(v.properties("name")));
                final List<String> values = IteratorUtils.list(v.values("name"));
                assertTrue(values.contains("marko"));
                assertTrue(values.contains("marko a. rodriguez"));
                assertTrue(values.contains("marko rodriguez"));
            });
            v.properties("name").forEachRemaining(Property::remove);
            tryCommit(g, g -> {
                assertEquals(0, IteratorUtils.count(v.properties()));
                assertEquals(0, IteratorUtils.count(v.properties("name")));
            });
            v.property("name", "marko");
            v.property("name", "marko a. rodriguez");
            v.property("name", "marko rodriguez");
            tryCommit(g, g -> {
                assertEquals(3, IteratorUtils.count(v.properties()));
                assertEquals(3, IteratorUtils.count(v.properties("name")));
                final List<String> values = IteratorUtils.list(v.values("name"));
                assertTrue(values.contains("marko"));
                assertTrue(values.contains("marko a. rodriguez"));
                assertTrue(values.contains("marko rodriguez"));
            });
            v.property(VertexProperty.Cardinality.single, "name", "okram", "acl", "private", "date", 2014);
            tryCommit(g, g -> {
                assertEquals(1, IteratorUtils.count(v.properties("name")));
                assertEquals(1, IteratorUtils.count(v.properties()));
                assertEquals(2, IteratorUtils.count(v.properties("name").next().properties()));
                final Map<String,Object> valueMap = IteratorUtils.collectMap(v.property("name").properties(), Property::key, Property::value);
                assertEquals("private", valueMap.get("acl"));
                assertEquals(2014, valueMap.get("date"));
            });

            v.remove();
            tryCommit(g, g -> {
                assertVertexEdgeCounts(0, 0);
            });

            final Vertex u = g.addVertex("name", "marko", "name", "marko a. rodriguez", "name", "marko rodriguez");
            tryCommit(g);
            u.properties().forEachRemaining(Property::remove);
            u.property(VertexProperty.Cardinality.single, "name", "okram", "acl", "private", "date", 2014);
            tryCommit(g, g -> {
                assertEquals(1, IteratorUtils.count(u.properties("name")));
                assertEquals(1, IteratorUtils.count(u.properties()));
                assertEquals(2, IteratorUtils.count(u.properties("name").next().properties()));
                final Map<String,Object> valueMap = IteratorUtils.collectMap(v.property("name").properties(), Property::key, Property::value);
                assertEquals("private", valueMap.get("acl"));
                assertEquals(2014, valueMap.get("date"));
            });

        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
        public void shouldHandleSetVertexProperties() {
            final Vertex v = g.addVertex("name", "marko", "name", "marko a. rodriguez");
            tryCommit(g, g -> {
                assertEquals(2, IteratorUtils.count(v.properties()));
                assertEquals(2, IteratorUtils.count(v.properties("name")));
                final List<String> values = IteratorUtils.list(v.values("name"));
                assertTrue(values.contains("marko"));
                assertTrue(values.contains("marko a. rodriguez"));
            });
            v.property(VertexProperty.Cardinality.set, "name", "marko rodriguez", "acl", "private");
            tryCommit(g, g -> {
                assertEquals(3, IteratorUtils.count(v.properties()));
                assertEquals(3, IteratorUtils.count(v.properties("name")));
                final List<String> values = IteratorUtils.list(v.values("name"));
                assertTrue(values.contains("marko"));
                assertTrue(values.contains("marko a. rodriguez"));
                assertTrue(values.contains("marko rodriguez"));
                final Iterator<VertexProperty<String>> iterator = v.properties("name");
                while (iterator.hasNext()) {
                    final VertexProperty<String> property = iterator.next();
                    if (property.value().equals("marko rodriguez")) {
                        assertEquals(1, IteratorUtils.count(property.properties()));
                        assertEquals("acl", property.properties().next().key());
                        assertEquals("private", property.properties().next().value());
                    } else {
                        assertEquals(0, IteratorUtils.count(property.properties()));
                    }
                }
            });
            ///
            v.property(VertexProperty.Cardinality.set, "name", "marko rodriguez", "acl", "public", "creator", "stephen");
            tryCommit(g, g -> {
                assertEquals(3, IteratorUtils.count(v.properties()));
                assertEquals(3, IteratorUtils.count(v.properties("name")));
                final List<String> values = IteratorUtils.list(v.values("name"));
                assertTrue(values.contains("marko"));
                assertTrue(values.contains("marko a. rodriguez"));
                assertTrue(values.contains("marko rodriguez"));
                final Iterator<VertexProperty<String>> iterator = v.properties("name");
                while (iterator.hasNext()) {
                    final VertexProperty<String> property = iterator.next();
                    if (property.value().equals("marko rodriguez")) {
                        assertEquals(2, IteratorUtils.count(property.properties()));
                        assertEquals("acl", property.properties("acl").next().key());
                        assertEquals("public", property.properties("acl").next().value());
                        assertEquals("creator", property.properties("creator").next().key());
                        assertEquals("stephen", property.properties("creator").next().value());
                    } else {
                        assertEquals(0, IteratorUtils.count(property.properties()));
                    }
                }
            });


        }


        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_STRING_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_BOOLEAN_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        public void shouldRespectWhatAreEdgesAndWhatArePropertiesInMultiProperties() {

            final Vertex marko = g.addVertex("name", "marko");
            final Vertex stephen = g.addVertex("name", "stephen");
            marko.addEdge("knows", stephen);
            final VertexProperty santaFe = marko.property("location", "santa fe", "visible", false);
            final VertexProperty newMexico = marko.property("location", "new mexico", "visible", true);

            assertEquals(1, IteratorUtils.count(marko.edges(Direction.OUT)));
            assertEquals(1, IteratorUtils.count(marko.edges(Direction.OUT, "knows")));
            assertEquals(3, IteratorUtils.count(marko.properties()));
            assertEquals(2, IteratorUtils.count(marko.properties("location")));
            assertEquals(1, IteratorUtils.count(marko.properties("name")));

            assertEquals(1, IteratorUtils.count(stephen.edges(Direction.IN)));
            assertEquals(1, IteratorUtils.count(stephen.edges(Direction.IN, "knows")));
            assertEquals(1, IteratorUtils.count(stephen.properties()));
            assertEquals(1, IteratorUtils.count(stephen.properties("name")));

            assertEquals(1, IteratorUtils.count(santaFe.properties()));
            assertEquals(1, IteratorUtils.count(santaFe.properties("visible")));
            assertEquals(0, IteratorUtils.count(santaFe.properties(T.key.getAccessor())));
            assertEquals(0, IteratorUtils.count(santaFe.properties(T.value.getAccessor())));

            assertEquals(1, IteratorUtils.count(newMexico.properties()));
            assertEquals(1, IteratorUtils.count(newMexico.properties("visible")));
            assertEquals(0, IteratorUtils.count(newMexico.properties(T.key.getAccessor())));
            assertEquals(0, IteratorUtils.count(newMexico.properties(T.value.getAccessor())));
        }
    }

    public static class VertexPropertyRemoval extends AbstractGremlinTest {

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_PROPERTY)
        public void shouldSupportIdempotentVertexPropertyRemoval() {
            final Vertex a = g.addVertex("name", "marko");
            final Vertex b = g.addVertex("name", "daniel", "name", "kuppitz");
            a.property("name").remove();
            a.property("name").remove();
            a.property("name").remove();
            b.properties("name").forEachRemaining(Property::remove);
            b.property("name").remove();
            b.properties("name").forEachRemaining(Property::remove);
            b.property("name").remove();
            b.properties("name").forEachRemaining(Property::remove);
            b.property("name").remove();
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_PROPERTY)
        public void shouldAllowIteratingAndRemovingVertexPropertyProperties() {
            final Vertex daniel = g.addVertex("name", "daniel", "name", "kuppitz", "name", "big d", "name", "the german");
            daniel.properties("name").forEachRemaining(vp ->  {
                vp.<Object>property("aKey", UUID.randomUUID().toString());
                vp.<Object>property("bKey", UUID.randomUUID().toString());
                vp.<Object>property("cKey", UUID.randomUUID().toString());
            });

            assertEquals(4, IteratorUtils.count(daniel.properties()));
            assertEquals(12, IteratorUtils.list(IteratorUtils.map(daniel.properties(), p -> IteratorUtils.count(p.properties()))).stream().mapToInt(kv -> kv.intValue()).sum());

            daniel.properties().forEachRemaining(Property::remove);
            assertEquals(4, IteratorUtils.count(daniel.properties()));
            assertEquals(0, IteratorUtils.list(IteratorUtils.map(daniel.properties(), p -> IteratorUtils.count(p.properties()))).stream().mapToInt(kv -> kv.intValue()).sum());

            daniel.properties().forEachRemaining(VertexProperty::remove);
            assertEquals(0, IteratorUtils.count(daniel.properties()));
            assertEquals(0, IteratorUtils.list(IteratorUtils.map(daniel.properties(), p -> IteratorUtils.count(p.properties()))).stream().mapToInt(kv -> kv.intValue()).sum());
        }


        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_PROPERTY)
        public void shouldRemoveMultiProperties() {
            final Vertex v = g.addVertex("name", "marko", "age", 34);
            v.property("name", "marko a. rodriguez");
            tryCommit(g);
            v.property("name", "marko rodriguez");
            v.property("name", "marko");
            tryCommit(g, g -> {
                assertEquals(5, IteratorUtils.count(v.properties()));
                assertEquals(4, IteratorUtils.count(v.properties("name")));
                final List<String> values = IteratorUtils.list(v.values("name"));
                assertThat(values, hasItem("marko a. rodriguez"));
                assertThat(values, hasItem("marko rodriguez"));
                assertThat(values, hasItem("marko"));
                assertVertexEdgeCounts(1, 0);
            });

            IteratorUtils.filter(v.properties(), p -> p.value().equals("marko")).forEachRemaining(VertexProperty::remove);
            tryCommit(g, g -> {
                assertEquals(3, IteratorUtils.count(v.properties()));
                assertEquals(2, IteratorUtils.count(v.properties("name")));
                assertVertexEdgeCounts(1, 0);
            });

            v.property("age").remove();
            tryCommit(g, g -> {
                assertEquals(2, IteratorUtils.count(v.properties()));
                assertEquals(2, IteratorUtils.count(v.properties("name")));
                assertVertexEdgeCounts(1, 0);
            });

            IteratorUtils.filter(v.properties("name"), p -> p.key().equals("name")).forEachRemaining(VertexProperty::remove);
            tryCommit(g, g -> {
                assertEquals(0, IteratorUtils.count(v.properties()));
                assertEquals(0, IteratorUtils.count(v.properties("name")));
                assertVertexEdgeCounts(1, 0);
            });
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
        public void shouldRemoveMultiPropertiesWhenVerticesAreRemoved() {
            final Vertex marko = g.addVertex("name", "marko", "name", "okram");
            final Vertex stephen = g.addVertex("name", "stephen", "name", "spmallette");
            tryCommit(g, g -> {
                assertVertexEdgeCounts(2, 0);
                assertEquals(2, IteratorUtils.count(marko.properties("name")));
                assertEquals(2, IteratorUtils.count(stephen.properties("name")));
                assertEquals(2, IteratorUtils.count(marko.properties()));
                assertEquals(2, IteratorUtils.count(stephen.properties()));
                assertEquals(0, IteratorUtils.count(marko.properties("blah")));
                assertEquals(0, IteratorUtils.count(stephen.properties("blah")));
            });

            stephen.remove();
            tryCommit(g, g -> {
                assertVertexEdgeCounts(1, 0);
                assertEquals(2, IteratorUtils.count(marko.properties("name")));
                assertEquals(2, IteratorUtils.count(marko.properties()));
                assertEquals(0, IteratorUtils.count(marko.properties("blah")));
            });

            for (int i = 0; i < 100; i++) {
                marko.property("name", i);
            }
            tryCommit(g, g -> {
                assertVertexEdgeCounts(1, 0);
                assertEquals(102, IteratorUtils.count(marko.properties("name")));
                assertEquals(102, IteratorUtils.count(marko.properties()));
                assertEquals(0, IteratorUtils.count(marko.properties("blah")));
            });

            g.V().properties("name").has(T.value, (a, b) -> ((Class) b).isAssignableFrom(a.getClass()), Integer.class).remove();
            tryCommit(g, g -> {
                assertVertexEdgeCounts(1, 0);
                assertEquals(2, IteratorUtils.count(marko.properties("name")));
                assertEquals(2, IteratorUtils.count(marko.properties()));
                assertEquals(0, IteratorUtils.count(marko.properties("blah")));
            });
            marko.remove();
            tryCommit(g, g -> {
                assertVertexEdgeCounts(0, 0);
            });
        }
    }

    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "elementAlreadyRemoved"
    })
    public static class ExceptionConsistencyWhenVertexPropertyRemovedTest extends AbstractGremlinTest {

        // todo: need to cover actual meta property assignment here as VertexTest doesn't check for that features
        /*
        {"v.property(k,v)", FunctionUtils.wrapConsumer((Vertex v) -> {
            v.property("k", "v");
        })},
        */

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {"property(k)", FunctionUtils.wrapConsumer((VertexProperty p) -> p.property("year"))}});
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public Consumer<VertexProperty> functionToTest;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
        public void shouldThrowExceptionIfVertexPropertyWasRemoved() {
            final Vertex v1 = g.addVertex();
            final VertexProperty p = v1.property("name", "stephen", "year", "2012");
            final Object id = p.id();
            p.remove();
            tryCommit(g, g -> {
                try {
                    functionToTest.accept(p);
                    fail("Should have thrown exception as the Vertex was already removed");
                } catch (Exception ex) {
                    validateException(Element.Exceptions.elementAlreadyRemoved(VertexProperty.class, id), ex);
                }
            });
        }
    }

    public static class VertexPropertyProperties extends AbstractGremlinTest {

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES)
        public void shouldSupportPropertiesOnMultiProperties() {
            final Vertex v = g.addVertex("name", "marko", "age", 34);
            tryCommit(g, g -> {
                assertEquals(2, IteratorUtils.count(v.properties()));
                assertVertexEdgeCounts(1, 0);
                // TODO: Neo4j needs a better ID system for VertexProperties
                assertEquals(v.property("name"), v.property("name").property("acl", "public").element());
                assertEquals(v.property("age"), v.property("age").property("acl", "private").element());
            });

            v.property("name").property("acl", "public");
            v.property("age").property("acl", "private");
            tryCommit(g, g -> {
                assertEquals(2, IteratorUtils.count(v.properties()));
                assertEquals(1, IteratorUtils.count(v.properties("age")));
                assertEquals(1, IteratorUtils.count(v.properties("name")));
                assertEquals(1, IteratorUtils.count(v.property("age").properties()));
                assertEquals(1, IteratorUtils.count(v.property("name").properties()));
                assertEquals(1, IteratorUtils.count(v.property("age").properties("acl")));
                assertEquals(1, IteratorUtils.count(v.property("name").properties("acl")));
                assertEquals("private", v.property("age").values("acl").next());
                assertEquals("public", v.property("name").values("acl").next());
                assertVertexEdgeCounts(1, 0);
            });

            v.property("age").property("acl", "public");
            v.property("age").property("changeDate", 2014);
            tryCommit(g, g -> {
                assertEquals("public", v.property("age").values("acl").next());
                assertEquals(2014, v.property("age").values("changeDate").next());
                assertEquals(1, IteratorUtils.count(v.properties("age")));
                assertEquals(2, IteratorUtils.count(v.properties("age").next().properties()));
            });
        }
    }
}
