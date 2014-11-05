package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.ExceptionCoverage;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.util.function.FunctionUtils;
import com.tinkerpop.gremlin.util.function.TriFunction;
import org.javatuples.Pair;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
        public void shouldAddMultiProperties() {
            final Vertex v = g.addVertex("name", "marko", "age", 34);
            tryCommit(g, g -> {
                assertEquals("marko", v.property("name").value());
                assertEquals("marko", v.value("name"));
                assertEquals(34, v.property("age").value());
                assertEquals(34, v.<Integer>value("age").intValue());
                assertEquals(1, v.properties("name").count().next().intValue());
                assertEquals(2, v.properties().count().next().intValue());
                assertEquals(1, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
            });

            final VertexProperty<String> property = v.property("name", "marko a. rodriguez");
            tryCommit(g, g -> assertEquals(v, property.element()));

            try {
                v.property("name");
                fail("This should throw a: " + Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name"));
            } catch (final Exception e) {
                validateException(Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name"), e);
            }

            assertTrue(v.valueMap().next().get("name").contains("marko"));
            assertTrue(v.valueMap().next().get("name").contains("marko a. rodriguez"));
            assertEquals(3, v.properties().count().next().intValue());
            assertEquals(2, v.properties("name").count().next().intValue());
            assertEquals(1, g.V().count().next().intValue());
            assertEquals(0, g.E().count().next().intValue());

            assertEquals(v, v.property("name", "mrodriguez").element());
            tryCommit(g, g -> {
                assertEquals(3, v.properties("name").count().next().intValue());
                assertEquals(4, v.properties().count().next().intValue());
                assertEquals(1, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
            });

            v.<String>properties("name").sideEffect(meta -> {
                meta.get().property("counter", meta.get().value().length());
                meta.get().property(Graph.Key.hide("counter"), meta.get().value().length());
            }).iterate();
            tryCommit(g, g -> {
                v.properties().forEachRemaining(meta -> {
                    assertEquals(meta.key(), meta.label());
                    assertTrue(meta.isPresent());
                    assertFalse(meta.isHidden());
                    assertEquals(v, meta.element());
                    if (meta.key().equals("age")) {
                        assertEquals(meta.value(), 34);
                        assertEquals(0, meta.properties().count().next().intValue());
                    }
                    if (meta.key().equals("name")) {
                        assertEquals(((String) meta.value()).length(), meta.<Integer>value("counter").intValue());
                        assertEquals(((String) meta.value()).length(), meta.<Integer>value(Graph.Key.hide("counter")).intValue());
                        assertEquals(1, meta.properties().count().next().intValue());
                        assertEquals(1, meta.keys().size());
                        assertTrue(meta.keys().contains("counter"));
                        assertEquals(1, meta.hiddens().count().next().intValue());
                        assertEquals(1, meta.hiddenKeys().size());
                        assertTrue(meta.hiddenKeys().contains("counter"));
                    }
                });
                assertEquals(1, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
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
                assertEquals(3, v.properties().count().next().intValue());
                assertEquals(3, v.properties("name").count().next().intValue());
                assertTrue(v.properties("name").value().toList().contains("marko"));
                assertTrue(v.properties("name").value().toList().contains("marko a. rodriguez"));
                assertTrue(v.properties("name").value().toList().contains("marko rodriguez"));
            });
            v.properties("name").remove();
            tryCommit(g, g -> {
                assertEquals(0, v.properties().count().next().intValue());
                assertEquals(0, v.properties("name").count().next().intValue());
            });
            v.property("name", "marko");
            v.property("name", "marko a. rodriguez");
            v.property("name", "marko rodriguez");
            tryCommit(g, g -> {
                assertEquals(3, v.properties().count().next().intValue());
                assertEquals(3, v.properties("name").count().next().intValue());
                assertTrue(v.properties("name").value().toList().contains("marko"));
                assertTrue(v.properties("name").value().toList().contains("marko a. rodriguez"));
                assertTrue(v.properties("name").value().toList().contains("marko rodriguez"));
            });
            v.singleProperty("name", "okram", "acl", "private", "date", 2014);
            tryCommit(g, g -> {
                assertEquals(1, v.properties("name").count().next().intValue());
                assertEquals(1, v.properties().count().next().intValue());
                assertEquals(2, v.property("name").valueMap().next().size());
                assertEquals("private", v.property("name").valueMap().next().get("acl"));
                assertEquals(2014, v.property("name").valueMap().next().get("date"));
            });

            v.remove();
            tryCommit(g, g -> {
                assertEquals(0, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
            });

            final Vertex u = g.addVertex("name", "marko", "name", "marko a. rodriguez", "name", "marko rodriguez");
            tryCommit(g);
            u.properties().remove();
            u.singleProperty("name", "okram", "acl", "private", "date", 2014);
            tryCommit(g, g -> {
                assertEquals(1, u.properties("name").count().next().intValue());
                assertEquals(1, u.properties().count().next().intValue());
                assertEquals(2, u.property("name").valueMap().next().size());
                assertEquals("private", u.property("name").valueMap().next().get("acl"));
                assertEquals(2014, u.property("name").valueMap().next().get("date"));
            });

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
            b.properties("name").remove();
            b.property("name").remove();
            b.properties("name").remove();
            b.property("name").remove();
            b.properties("name").remove();
            b.property("name").remove();
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_PROPERTY)
        public void shouldAllowIteratingAndRemovingVertexPropertyProperties() {
            final Vertex daniel = g.addVertex("name", "daniel", "name", "kuppitz", "name", "big d", "name", "the german");
            daniel.properties("name")
                    .sideEffect(vp -> vp.get().<Object>property("aKey", UUID.randomUUID().toString()))
                    .sideEffect(vp -> vp.get().<Object>property("bKey", UUID.randomUUID().toString()))
                    .sideEffect(vp -> vp.get().<Object>property("cKey", UUID.randomUUID().toString())).iterate();

            assertEquals(4, daniel.properties().count().next().longValue());
            assertEquals(12, daniel.properties().properties().count().next().longValue());

            daniel.properties().properties().remove();
            assertEquals(4, daniel.properties().count().next().longValue());
            assertEquals(0, daniel.properties().properties().count().next().longValue());

            daniel.properties().remove();
            assertEquals(0, daniel.properties().count().next().longValue());
            assertEquals(0, daniel.properties().properties().count().next().longValue());
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
                assertEquals(5, v.properties().count().next().intValue());
                assertEquals(4, v.properties().has(T.key, "name").count().next().intValue());
                assertEquals(4, v.properties("name").count().next().intValue());
                assertEquals(1, v.properties("name").has(T.value, "marko a. rodriguez").count().next().intValue());
                assertEquals(1, v.properties("name").has(T.value, "marko rodriguez").count().next().intValue());
                assertEquals(2, v.properties("name").has(T.value, "marko").count().next().intValue());
                assertEquals(1, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
            });

            v.properties().has(T.value, "marko").remove();
            tryCommit(g, g -> {
                assertEquals(3, v.properties().count().next().intValue());
                assertEquals(2, v.properties().has(T.key, "name").count().next().intValue());
                assertEquals(1, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
            });

            v.property("age").remove();
            tryCommit(g, g -> {
                assertEquals(2, v.properties().count().next().intValue());
                assertEquals(2, v.properties().has(T.key, "name").count().next().intValue());
                assertEquals(1, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
            });

            v.properties("name").has(T.key, "name").remove();
            tryCommit(g, g -> {
                assertEquals(0, v.properties().count().next().intValue());
                assertEquals(0, v.properties().has(T.key, "name").count().next().intValue());
                assertEquals(1, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
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
                assertEquals(2, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
                assertEquals(2, marko.properties("name").count().next().intValue());
                assertEquals(2, stephen.properties("name").count().next().intValue());
                assertEquals(2, marko.properties().count().next().intValue());
                assertEquals(2, stephen.properties().count().next().intValue());
                assertEquals(0, marko.properties("blah").count().next().intValue());
                assertEquals(0, stephen.properties("blah").count().next().intValue());
                assertEquals(2, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
            });

            stephen.remove();
            tryCommit(g, g -> {
                assertEquals(1, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
                assertEquals(2, marko.properties("name").count().next().intValue());
                assertEquals(2, marko.properties().count().next().intValue());
                assertEquals(0, marko.properties("blah").count().next().intValue());
            });

            for (int i = 0; i < 100; i++) {
                marko.property("name", i);
            }
            tryCommit(g, g -> {
                assertEquals(1, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
                assertEquals(102, marko.properties("name").count().next().intValue());
                assertEquals(102, marko.properties().count().next().intValue());
                assertEquals(0, marko.properties("blah").count().next().intValue());
            });

            g.V().properties("name").has(T.value, (a, b) -> ((Class) b).isAssignableFrom(a.getClass()), Integer.class).remove();
            tryCommit(g, g -> {
                assertEquals(1, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
                assertEquals(2, marko.properties("name").count().next().intValue());
                assertEquals(2, marko.properties().count().next().intValue());
                assertEquals(0, marko.properties("blah").count().next().intValue());
            });
            marko.remove();
            tryCommit(g, g -> {
                assertEquals(0, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
            });
        }
    }

    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "elementAlreadyRemoved"
    })
    public static class ExceptionConsistencyWhenVertexPropertyRemovedTest extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "{index}: expect - {0}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {"property(k)", FunctionUtils.wrapConsumer((VertexProperty p) -> p.property("name"))}});
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public Consumer<VertexProperty> functionToTest;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
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
                assertEquals(2, g.V().properties().count().next().intValue());
                assertEquals(1, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
                // TODO: Neo4j needs a better ID system for VertexProperties
                assertEquals(v.property("name"), v.property("name").property("acl", "public").element());
                assertEquals(v.property("age"), v.property("age").property("acl", "private").element());
            });

            v.property("name").property("acl", "public");
            v.property("age").property("acl", "private");
            tryCommit(g, g -> {

                assertEquals(2, g.V().properties().count().next().intValue());
                assertEquals(1, g.V().properties("age").count().next().intValue());
                assertEquals(1, g.V().properties("name").count().next().intValue());
                assertEquals(1, g.V().properties("age").properties().count().next().intValue());
                assertEquals(1, g.V().properties("name").properties().count().next().intValue());
                assertEquals(1, g.V().properties("age").properties("acl").count().next().intValue());
                assertEquals(1, g.V().properties("name").properties("acl").count().next().intValue());
                assertEquals("private", g.V().properties("age").properties("acl").value().next());
                assertEquals("public", g.V().properties("name").properties("acl").value().next());
                assertEquals("private", g.V().properties("age").values("acl").next());
                assertEquals("public", g.V().properties("name").values("acl").next());
                assertEquals(1, g.V().count().next().intValue());
                assertEquals(0, g.E().count().next().intValue());
            });

            v.property("age").property("acl", "public");
            v.property("age").property("changeDate", 2014);
            tryCommit(g, g -> {
                assertEquals("public", g.V().properties("age").values("acl").next());
                assertEquals(2014, g.V().properties("age").values("changeDate").next());
                assertEquals(1, v.properties("age").valueMap().count().next().intValue());
                assertEquals(2, v.properties("age").valueMap().next().size());
                assertTrue(v.properties("age").valueMap().next().containsKey("acl"));
                assertTrue(v.properties("age").valueMap().next().containsKey("changeDate"));
                assertEquals("public", v.properties("age").valueMap().next().get("acl"));
                assertEquals(2014, v.properties("age").valueMap().next().get("changeDate"));
            });
        }
    }


    public static class VertexPropertyTraversals extends AbstractGremlinTest {
        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES)
        public void shouldHandleVertexPropertyTraversals() {
            final Vertex v = g.addVertex("i", 1, "i", 2, "i", 3);
            tryCommit(g, g -> {
                assertEquals(3, v.properties().count().next().intValue());
                assertEquals(3, v.properties("i").count().next().intValue());
            });

            v.properties("i").sideEffect(m -> m.get().<Object>property("aKey", "aValue")).iterate();
            v.properties("i").properties("aKey").forEachRemaining(p -> assertEquals("aValue", p.value()));
            tryCommit(g, g -> {
                assertEquals(3, v.properties("i").properties("aKey").count().next().intValue());
                assertEquals(3, g.V().properties("i").properties("aKey").count().next().intValue());
                assertEquals(1, g.V().properties("i").has(T.value, 1).properties("aKey").count().next().intValue());
                assertEquals(3, g.V().properties("i").has(T.key, "i").properties().count().next().intValue());
            });
        }
    }

    @RunWith(Parameterized.class)
    public static class VertexPropertiesShouldHideCorrectlyGivenMultiProperty extends AbstractGremlinTest {

        @Parameterized.Parameters(name = "{index}: {0}")
        public static Iterable<Object[]> data() {
            final List<Pair<String, TriFunction<Graph, Vertex, Boolean, Boolean>>> tests = new ArrayList<>();
            tests.add(Pair.with("v.property(\"age\").isPresent()", (Graph g, Vertex v, Boolean multi) -> v.property("age").isPresent()));
            tests.add(Pair.with("v.value(\"age\").equals(16)", (Graph g, Vertex v, Boolean multi) -> v.value("age").equals(16)));
            tests.add(Pair.with("v.properties(\"age\").count().next().intValue() == 1", (Graph g, Vertex v, Boolean multi) -> v.properties("age").count().next().intValue() == 1));
            tests.add(Pair.with("v.properties(\"age\").value().next().equals(16)", (Graph g, Vertex v, Boolean multi) -> v.properties("age").value().next().equals(16)));
            tests.add(Pair.with("v.hiddens(\"age\").count().next().intValue() == 2", (Graph g, Vertex v, Boolean multi) -> v.hiddens("age").count().next().intValue() == (multi ? 2 : 1)));
            tests.add(Pair.with("v.hiddens(Graph.Key.hide(\"age\")).count().next().intValue() == 0", (Graph g, Vertex v, Boolean multi) -> v.hiddens(Graph.Key.hide("age")).count().next().intValue() == 0));
            tests.add(Pair.with("v.properties(Graph.Key.hide(\"age\")).count().next() == 0", (Graph g, Vertex v, Boolean multi) -> v.properties(Graph.Key.hide("age")).count().next().intValue() == 0));
            tests.add(Pair.with("v.propertyMap(Graph.Key.hide(\"age\")).next().size() == 0", (Graph g, Vertex v, Boolean multi) -> v.propertyMap(Graph.Key.hide("age")).next().size() == 0));
            tests.add(Pair.with("v.valueMap(Graph.Key.hide(\"age\")).next().size() == 0", (Graph g, Vertex v, Boolean multi) -> v.valueMap(Graph.Key.hide("age")).next().size() == 0));
            tests.add(Pair.with("v.propertyMap(\"age\").next().size() == 1", (Graph g, Vertex v, Boolean multi) -> v.propertyMap("age").next().size() == 1));
            tests.add(Pair.with("v.valueMap(\"age\").next().size() == 1", (Graph g, Vertex v, Boolean multi) -> v.valueMap("age").next().size() == 1));
            tests.add(Pair.with("v.hiddenMap(Graph.Key.hide(\"age\")).next().size() == 0", (Graph g, Vertex v, Boolean multi) -> v.hiddenMap(Graph.Key.hide("age")).next().size() == 0));
            tests.add(Pair.with("v.hiddenMap(\"age\").next().size() == 1", (Graph g, Vertex v, Boolean multi) -> v.hiddenMap("age").next().size() == 1));
            tests.add(Pair.with("v.hiddenValueMap(Graph.Key.hide(\"age\")).next().size() == 0", (Graph g, Vertex v, Boolean multi) -> v.hiddenValueMap(Graph.Key.hide("age")).next().size() == 0));
            tests.add(Pair.with("v.hiddenValueMap(\"age\").next().size() == 1", (Graph g, Vertex v, Boolean multi) -> v.hiddenValueMap("age").next().size() == 1));
            tests.add(Pair.with("v.hiddens(\"age\").value().toList().contains(34)", (Graph g, Vertex v, Boolean multi) -> multi ? v.hiddens("age").value().toList().contains(34) : v.hiddens("age").value().toList().contains(29)));
            tests.add(Pair.with("v.hiddens(\"age\").value().toList().contains(29)", (Graph g, Vertex v, Boolean multi) -> v.hiddens("age").value().toList().contains(29)));
            tests.add(Pair.with("v.hiddenKeys().size() == 2", (Graph g, Vertex v, Boolean multi) -> v.hiddenKeys().size() == 2));
            tests.add(Pair.with("v.keys().size() == 3", (Graph g, Vertex v, Boolean multi) -> v.keys().size() == 3));
            tests.add(Pair.with("v.keys().contains(\"age\")", (Graph g, Vertex v, Boolean multi) -> v.keys().contains("age")));
            tests.add(Pair.with("v.keys().contains(\"name\")", (Graph g, Vertex v, Boolean multi) -> v.keys().contains("name")));
            tests.add(Pair.with("v.hiddenKeys().contains(\"age\")", (Graph g, Vertex v, Boolean multi) -> v.hiddenKeys().contains("age")));
            tests.add(Pair.with("v.property(Graph.Key.hide(\"color\")).key().equals(\"color\")", (Graph g, Vertex v, Boolean multi) -> v.property(Graph.Key.hide("color")).key().equals("color")));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().propertyIterator(Graph.Key.hide(\"color\"))).count() == 0", (Graph g, Vertex v, Boolean multi) -> StreamFactory.stream(v.iterators().propertyIterator(Graph.Key.hide("color"))).count() == 0));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().propertyIterator(Graph.Key.hide(\"age\"))).count() == 0", (Graph g, Vertex v, Boolean multi) -> StreamFactory.stream(v.iterators().propertyIterator(Graph.Key.hide("age"))).count() == 0));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().propertyIterator(\"age\")).count() == 1", (Graph g, Vertex v, Boolean multi) -> StreamFactory.stream(v.iterators().propertyIterator("age")).count() == 1));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().hiddenPropertyIterator(Graph.Key.hide(\"color\"))).count() == 0", (Graph g, Vertex v, Boolean multi) -> StreamFactory.stream(v.iterators().hiddenPropertyIterator(Graph.Key.hide("color"))).count() == 0));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().hiddenPropertyIterator(Graph.Key.hide(\"age\"))).count() == 0", (Graph g, Vertex v, Boolean multi) -> StreamFactory.stream(v.iterators().hiddenPropertyIterator(Graph.Key.hide("age"))).count() == 0));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().hiddenPropertyIterator(\"color\")).count() == 1", (Graph g, Vertex v, Boolean multi) -> StreamFactory.stream(v.iterators().hiddenPropertyIterator("color")).count() == 1));
            tests.add(Pair.with("StreamFactory.stream(v.iterators().hiddenPropertyIterator(\"age\")).count() == 2", (Graph g, Vertex v, Boolean multi) -> StreamFactory.stream(v.iterators().hiddenPropertyIterator("age")).count() == (multi ? 2 : 1)));

            return tests.stream().map(d -> {
                final Object[] o = new Object[2];
                o[0] = d.getValue0();
                o[1] = d.getValue1();
                return o;
            }).collect(Collectors.toList());
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public TriFunction<Graph, Vertex, Boolean, Boolean> streamGetter;

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
        public void shouldHandleHiddenVertexMultiProperties() {
            final Vertex v = g.addVertex(Graph.Key.hide("age"), 34, Graph.Key.hide("age"), 29, "age", 16, "name", "marko", "food", "taco", Graph.Key.hide("color"), "purple");
            tryCommit(g, g -> {
                assertTrue(streamGetter.apply(g, v, true));
            });
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES)
        public void shouldHandleHiddenVertexProperties() {
            final Vertex v = g.addVertex(Graph.Key.hide("age"), 29, "age", 16, "name", "marko", "food", "taco", Graph.Key.hide("color"), "purple");
            tryCommit(g, g -> {
                assertTrue(streamGetter.apply(g, v, false));
            });
        }
    }
}
