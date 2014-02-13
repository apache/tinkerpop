package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.GraphAnnotationFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexAnnotationFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import com.tinkerpop.gremlin.structure.strategy.GraphStrategy;
import com.tinkerpop.gremlin.structure.strategy.PartitionGraphStrategy;
import com.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Optional;

import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_ANNOTATIONS;
import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_COMPUTER;
import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_STRATEGY;
import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

/**
 * Tests that do basic validation of proper Feature settings in Graph implementations.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class FeatureSupportTest  {
    private static final String INVALID_FEATURE_SPECIFICATION = "Features for %s specify that %s is false, but the feature appears to be implemented.  Reconsider this setting or throw the standard Exception.";

    /**
     * Feature checks that test {@link com.tinkerpop.gremlin.structure.Graph} functionality to determine if a feature should be on when it is marked
     * as not supported.
     */
    @ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
            "graphAnnotationsNotSupported",
            "graphComputerNotSupported",
            "transactionsNotSupported",
            "graphStrategyNotSupported"
    })
    public static class GraphFunctionalityTest extends AbstractGremlinTest {

        /**
         * A {@link com.tinkerpop.gremlin.structure.Graph} that does not support {@link com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures#FEATURE_COMPUTER} must call
         * {@link com.tinkerpop.gremlin.structure.Graph.Exceptions#graphComputerNotSupported()}.
         */
        @Test
        @FeatureRequirement(featureClass = GraphFeatures.class, feature = FEATURE_COMPUTER, supported = false)
        public void ifAGraphCanComputeThenItMustSupportComputer() throws Exception {
            try {
                g.compute();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphFeatures.class.getSimpleName(), FEATURE_COMPUTER));
            } catch (UnsupportedOperationException e) {
                assertEquals(Graph.Exceptions.graphComputerNotSupported().getMessage(), e.getMessage());
            }
        }

        /**
         * A {@link com.tinkerpop.gremlin.structure.Graph} that does not support {@link com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures#FEATURE_TRANSACTIONS} must call
         * {@link com.tinkerpop.gremlin.structure.Graph.Exceptions#transactionsNotSupported()}.
         */
        @Test
        @FeatureRequirement(featureClass = GraphFeatures.class, feature = FEATURE_TRANSACTIONS, supported = false)
        public void ifAGraphConstructsATxThenItMustSupportTransactions() throws Exception {
            try {
                g.tx();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphFeatures.class.getSimpleName(), FEATURE_TRANSACTIONS));
            } catch (UnsupportedOperationException e) {
                assertEquals(Graph.Exceptions.transactionsNotSupported().getMessage(), e.getMessage());
            }
        }

        /**
         * A {@link com.tinkerpop.gremlin.structure.Graph} that does not support {@link com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures#FEATURE_ANNOTATIONS} must call
         * {@link com.tinkerpop.gremlin.structure.Graph.Exceptions#graphAnnotationsNotSupported()}.
         */
        @Test
        @FeatureRequirement(featureClass = GraphFeatures.class, feature = FEATURE_STRATEGY, supported = false)
        public void ifAGraphAcceptsAnnotationsThenItMustSupportAnnotations() throws Exception {
            try {
                g.strategy();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphFeatures.class.getSimpleName(), FEATURE_ANNOTATIONS));
            } catch (UnsupportedOperationException e) {
                assertEquals(Graph.Exceptions.graphAnnotationsNotSupported().getMessage(), e.getMessage());
            }
        }

        /**
         * A {@link com.tinkerpop.gremlin.structure.Graph} that does not support {@link com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures#FEATURE_STRATEGY} must call
         * {@link com.tinkerpop.gremlin.structure.Graph.Exceptions#graphStrategyNotSupported()}.
         */
        @Test
        @FeatureRequirement(featureClass = GraphFeatures.class, feature = FEATURE_STRATEGY, supported = false)
        public void ifAGraphAcceptsStrategyThenItMustSupportStrategy() throws Exception {
            try {
                g.strategy();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphFeatures.class.getSimpleName(), FEATURE_STRATEGY));
            } catch (UnsupportedOperationException e) {
                assertEquals(Graph.Exceptions.graphStrategyNotSupported().getMessage(), e.getMessage());
            }
        }

        /**
         * If given a non-empty {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy} a graph that does not support
         * {@link com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures#FEATURE_STRATEGY} should throw
         * {@link com.tinkerpop.gremlin.structure.Graph.Exceptions#graphStrategyNotSupported()}.
         */
        @Test
        @FeatureRequirement(featureClass = GraphFeatures.class, feature = FEATURE_STRATEGY, supported = false)
        public void shouldThrowUnsupportedIfStrategyIsNonEmptyAndStrategyFeatureDisabled() {
            try {
                GraphFactory.open(config, Optional.<GraphStrategy>of(new PartitionGraphStrategy("k", "v")));
                fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphFeatures.class.getSimpleName(), FEATURE_STRATEGY));
            } catch (UnsupportedOperationException ex) {
                assertEquals(Graph.Exceptions.graphStrategyNotSupported().getMessage(), ex.getMessage());
            }

        }
    }

    /**
     * Feature checks that test {@link com.tinkerpop.gremlin.structure.Vertex} functionality to determine if a feature should be on when it is marked
     * as not supported.
     */
    public static class VertexFunctionalityTest extends AbstractGremlinTest {

        @Test
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void ifAnIdCanBeAssignedToVertexThenItMustSupportUserSuppliedIds() throws Exception {
            final Vertex v = g.addVertex(Element.ID, GraphManager.get().convertId(99999943835l));
            tryCommit(g, graph -> assertThat(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_USER_SUPPLIED_IDS),
                    v.getId().toString(),
                    is(not("99999943835"))));
        }
    }

    /**
     * Feature checks that test {@link com.tinkerpop.gremlin.structure.Element} {@link com.tinkerpop.gremlin.structure.Property} functionality to determine if a feature should be on
     * when it is marked as not supported.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "dataTypeOfPropertyValueNotSupported"
    })
    public static class ElementPropertyFunctionalityTest extends AbstractGremlinTest {
        private static final String INVALID_FEATURE_SPECIFICATION = "Features for %s specify that %s is false, but the feature appears to be implemented.  Reconsider this setting or throw the standard Exception.";

        @Parameterized.Parameters(name = "{index}: supports{0}({1})")
        public static Iterable<Object[]> data() {
            return PropertyTest.PropertyFeatureSupportTest.data();
        }

        @Parameterized.Parameter(value = 0)
        public String featureName;

        @Parameterized.Parameter(value = 1)
        public Object value;

        @Test
        public void shouldEnableFeatureOnEdgeIfNotEnabled() throws Exception {
            assumeThat(g.getFeatures().supports(EdgePropertyFeatures.class, featureName), is(false));
            try {
                final Edge edge = createEdgeForPropertyFeatureTests();
                edge.setProperty("key", value);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgePropertyFeatures.class.getSimpleName(), featureName));
            } catch (UnsupportedOperationException e) {
                assertEquals(Property.Exceptions.dataTypeOfPropertyValueNotSupported(value).getMessage(), e.getMessage());
            }
        }

        @Test
        public void shouldEnableFeatureOnVertexIfNotEnabled() throws Exception {
            assumeThat(g.getFeatures().supports(VertexPropertyFeatures.class, featureName), is(false));
            try {
                g.addVertex("key", value);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), featureName));
            } catch (UnsupportedOperationException e) {
                assertEquals(Property.Exceptions.dataTypeOfPropertyValueNotSupported(value).getMessage(), e.getMessage());
            }
        }

        private Edge createEdgeForPropertyFeatureTests() {
            final Vertex vertexA = g.addVertex();
            final Vertex vertexB = g.addVertex();
            return vertexA.addEdge(GraphManager.get().convertLabel("knows"), vertexB);
        }
    }

    /**
     * Feature checks that test {@link com.tinkerpop.gremlin.structure.Graph} {@link com.tinkerpop.gremlin.structure.Graph.Annotations} and {@link com.tinkerpop.gremlin.structure.Vertex} {@link com.tinkerpop.gremlin.structure.AnnotatedList}
     * functionality to determine if a feature should be on when it is marked as not supported.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Graph.Annotations.Exceptions.class, methods = {
            "dataTypeOfGraphAnnotationValueNotSupported"
    })
    public static class AnnotationFunctionalityTest extends AbstractGremlinTest {
        private static final String INVALID_FEATURE_SPECIFICATION = "Features for %s specify that %s is false, but the feature appears to be implemented.  Reconsider this setting or throw the standard Exception.";

        @Parameterized.Parameters(name = "{index}: supports{0}({1})")
        public static Iterable<Object[]> data() {
            return AnnotationTest.AnnotationFeatureSupportTest.data();
        }

        @Parameterized.Parameter(value = 0)
        public String featureName;

        @Parameterized.Parameter(value = 1)
        public Object value;

        @Test
        public void shouldEnableFeatureOnGraphIfNotEnabled() throws Exception {
            assumeThat(g.getFeatures().supports(GraphAnnotationFeatures.class, featureName), is(false));
            try {
               final Graph.Annotations annotations = g.annotations();
                annotations.set("key", value);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphAnnotationFeatures.class.getSimpleName(), featureName));
            } catch (UnsupportedOperationException e) {
                assertEquals(Graph.Annotations.Exceptions.dataTypeOfGraphAnnotationValueNotSupported(value).getMessage(), e.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = VertexAnnotationFeatures.class, feature = VertexAnnotationFeatures.FEATURE_STRING_VALUES)
        public void shouldEnableFeatureOnVertexIfNotEnabled() throws Exception {
            assumeThat(g.getFeatures().supports(VertexPropertyFeatures.class, featureName), is(false));
            try {
                final Vertex v = g.addVertex("key", AnnotatedList.make());
                final Property<AnnotatedList<String>> al = v.getProperty("key");
                al.get().addValue("v", "t", value);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), featureName));
            } catch (UnsupportedOperationException e) {
                assertEquals(Graph.Annotations.Exceptions.dataTypeOfGraphAnnotationValueNotSupported(value).getMessage(), e.getMessage());
            }
        }
    }

    /**
     * Feature checks that simply evaluate conflicting feature definitions without evaluating the actual implementation
     * itself.
     */
    public static class LogicalFeatureSupportTest extends AbstractGremlinTest {

        private EdgeFeatures edgeFeatures;
        private EdgePropertyFeatures edgePropertyFeatures;
        private GraphFeatures graphFeatures;
        private GraphAnnotationFeatures graphAnnotationFeatures;
        private VertexFeatures vertexFeatures;
        private VertexAnnotationFeatures vertexAnnotationFeatures;
        private VertexPropertyFeatures vertexPropertyFeatures;

        @Before
        public void innerSetup() {
            final Graph.Features f = g.getFeatures();
            edgeFeatures = f.edge();
            edgePropertyFeatures = edgeFeatures.properties();
            graphFeatures = f.graph();
            graphAnnotationFeatures = graphFeatures.annotations();
            vertexFeatures = f.vertex();
            vertexAnnotationFeatures = vertexFeatures.annotations();
            vertexPropertyFeatures = vertexFeatures.properties();
        }

        @Test
        public void ifGraphHasAnnotationsEnabledThenItMustSupportADataType() {
            assertTrue(graphAnnotationFeatures.supportsAnnotations()
                    && (graphAnnotationFeatures.supportsBooleanValues() || graphAnnotationFeatures.supportsDoubleValues()
                    || graphAnnotationFeatures.supportsFloatValues() || graphAnnotationFeatures.supportsIntegerValues()
                    || graphAnnotationFeatures.supportsLongValues() || graphAnnotationFeatures.supportsMapValues()
                    || graphAnnotationFeatures.supportsMetaProperties() || graphAnnotationFeatures.supportsMixedListValues()
                    || graphAnnotationFeatures.supportsPrimitiveArrayValues() || graphAnnotationFeatures.supportsPrimitiveArrayValues()
                    || graphAnnotationFeatures.supportsSerializableValues() || graphAnnotationFeatures.supportsStringValues()
                    || graphAnnotationFeatures.supportsUniformListValues()));
        }

        @Test
        public void ifVertexHasAnnotationsEnabledThenItMustSupportADataType() {
            assertTrue(vertexAnnotationFeatures.supportsAnnotations()
                    && (vertexAnnotationFeatures.supportsBooleanValues() || vertexAnnotationFeatures.supportsDoubleValues()
                    || vertexAnnotationFeatures.supportsFloatValues() || vertexAnnotationFeatures.supportsIntegerValues()
                    || vertexAnnotationFeatures.supportsLongValues() || vertexAnnotationFeatures.supportsMapValues()
                    || vertexAnnotationFeatures.supportsMetaProperties() || vertexAnnotationFeatures.supportsMixedListValues()
                    || vertexAnnotationFeatures.supportsPrimitiveArrayValues() || vertexAnnotationFeatures.supportsPrimitiveArrayValues()
                    || vertexAnnotationFeatures.supportsSerializableValues() || vertexAnnotationFeatures.supportsStringValues()
                    || vertexAnnotationFeatures.supportsUniformListValues()));
        }

        @Test
        public void ifEdgeHasPropertyEnabledThenItMustSupportADataType() {
            assertTrue(edgePropertyFeatures.supportsProperties()
                    && (edgePropertyFeatures.supportsBooleanValues() || edgePropertyFeatures.supportsDoubleValues()
                    || edgePropertyFeatures.supportsFloatValues() || edgePropertyFeatures.supportsIntegerValues()
                    || edgePropertyFeatures.supportsLongValues() || edgePropertyFeatures.supportsMapValues()
                    || edgePropertyFeatures.supportsMetaProperties() || edgePropertyFeatures.supportsMixedListValues()
                    || edgePropertyFeatures.supportsPrimitiveArrayValues() || edgePropertyFeatures.supportsPrimitiveArrayValues()
                    || edgePropertyFeatures.supportsSerializableValues() || edgePropertyFeatures.supportsStringValues()
                    || edgePropertyFeatures.supportsUniformListValues()));
        }

        @Test
        public void ifVertexHasPropertyEnabledThenItMustSupportADataType() {
            assertTrue(vertexPropertyFeatures.supportsProperties()
            && (vertexPropertyFeatures.supportsBooleanValues() || vertexPropertyFeatures.supportsDoubleValues()
                    || vertexPropertyFeatures.supportsFloatValues() || vertexPropertyFeatures.supportsIntegerValues()
                    || vertexPropertyFeatures.supportsLongValues() || vertexPropertyFeatures.supportsMapValues()
                    || vertexPropertyFeatures.supportsMetaProperties() || vertexPropertyFeatures.supportsMixedListValues()
                    || vertexPropertyFeatures.supportsPrimitiveArrayValues() || vertexPropertyFeatures.supportsPrimitiveArrayValues()
                    || vertexPropertyFeatures.supportsSerializableValues() || vertexPropertyFeatures.supportsStringValues()
                    || vertexPropertyFeatures.supportsUniformListValues()));
        }


    }
}
