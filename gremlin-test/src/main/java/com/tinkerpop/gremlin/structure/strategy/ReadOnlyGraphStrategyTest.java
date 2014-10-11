package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.junit.Test;

import static com.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_INTEGER_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES;
import static com.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures.FEATURE_VARIABLES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReadOnlyGraphStrategyTest extends AbstractGremlinTest {
    private static final GraphStrategy readOnlyGraphStrategy = new ReadOnlyGraphStrategy();

    public ReadOnlyGraphStrategyTest() {
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    public void shouldNotAllowAddVertex() {
        assertException(g -> g.addVertex());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    public void shouldNotAllowRemoveVertex() {
        g.addVertex();
        assertException(g -> g.V().next().remove());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    public void shouldNotAllowRemoveEdge() {
        final Vertex v = g.addVertex();
        v.addEdge("friend", v);
        assertException(g -> {
            final Edge e = g.E().next();
            e.remove();
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    public void shouldNotAllowAddEdge() {
        g.addVertex();
        assertException(g -> {
            final Vertex v = g.V().next();
            v.addEdge("friend", v);
        });
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowVertexSetProperties() {
        g.addVertex();
        assertException(g -> g.V().next().<String>property("test", "test"));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotAllowVertexSetProperty() {
        g.addVertex();
        assertException(g -> g.V().next().<String>property("test", "test"));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldNotAllowVertexPropertySetProperty() {
        g.addVertex();
        assertException(g -> {
            final VertexProperty p = g.V().next().<String>property("test", "test");
            p.property("property", "on-a-property");
        });
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowEdgeSetProperties() {
        final Vertex v = g.addVertex();
        v.addEdge("friend", v);
        assertException(g -> g.E().next().<String>property("test", "test"));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowEdgeSetProperty() {
        final Vertex v = g.addVertex();
        v.addEdge("friend", v);
        assertException(g -> g.E().next().<String>property("test", "test"));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowVertexPropertyRemoval() {
        final Vertex v = g.addVertex();
        v.property("test", "test");
        final Property<String> p = v.property("test");
        assertEquals("test", p.value());

        assertException(g -> {
            final VertexProperty<Object> prop = g.V().next().iterators().propertyIterator("test").next();
            prop.remove();
        });
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowEdgePropertyRemoval() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("friend", v);
        e.property("test", "test");
        final Property<String> p = e.property("test");
        assertEquals("test", p.value());

        assertException(g -> {
            final Property<String> prop = g.E().next().property("test");
            prop.remove();
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VariableFeatures.class, feature = FEATURE_VARIABLES)
    public void shouldNotAllowVariableModifications() {
        assertException(g -> {
            g.variables().set("will", "not work");
        });
    }

    @Test(expected = UnsupportedOperationException.class)
    @FeatureRequirement(featureClass = Graph.Features.VariableFeatures.class, feature = FEATURE_VARIABLES)
    public void shouldNotAllowVariableAsMapModifications() {
        g.variables().set("will", "be read-only");
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(readOnlyGraphStrategy);
        swg.variables().asMap().put("will", "not work");
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldReturnWrappedVertexToString() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        final Vertex v1 = swg.addVertex(T.label, "Person");
        final Vertex originalVertex = ((StrategyWrappedVertex) v1).getBaseVertex();
        swg.strategy().setGraphStrategy(readOnlyGraphStrategy);
        assertEquals(StringFactory.graphStrategyVertexString(swg.strategy().getGraphStrategy().get(), originalVertex), v1.toString());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldReturnWrappedEdgeToString() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        final Vertex v1 = swg.addVertex(T.label, "Person");
        final Vertex v2 = swg.addVertex(T.label, "Person");
        final Edge e1 = v1.addEdge("friend", v2);
        final Edge originalEdge = ((StrategyWrappedEdge) e1).getBaseEdge();
        swg.strategy().setGraphStrategy(readOnlyGraphStrategy);
        assertEquals(StringFactory.graphStrategyEdgeString(swg.strategy().getGraphStrategy().get(), originalEdge), e1.toString());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldReturnWrappedVertexPropertyToString() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        final Vertex v1 = swg.addVertex(T.label, "Person", "age", "one");
        final VertexProperty age = v1.property("age");
        final Vertex originalVertex = ((StrategyWrappedVertex) v1).getBaseVertex();
        final VertexProperty originalVertexProperty = originalVertex.property("age");
        swg.strategy().setGraphStrategy(readOnlyGraphStrategy);
        assertEquals(StringFactory.graphStrategyPropertyString(swg.strategy().getGraphStrategy().get(), originalVertexProperty), age.toString());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldReturnWrappedPropertyToString() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        final Vertex v1 = swg.addVertex(T.label, "Person");
        final Vertex v2 = swg.addVertex(T.label, "Person");
        final Edge e1 = v1.addEdge("friend", v2, "weight", "fifty");
        final Property weight = e1.property("weight");
        final Edge originalEdge = ((StrategyWrappedEdge) e1).getBaseEdge();
        final Property originalProperty = originalEdge.property("weight");
        swg.strategy().setGraphStrategy(readOnlyGraphStrategy);
        assertEquals(StringFactory.graphStrategyPropertyString(swg.strategy().getGraphStrategy().get(), originalProperty), weight.toString());
    }

    @Test
    public void shouldReturnWrappedToString() {
        final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
        swg.strategy().setGraphStrategy(readOnlyGraphStrategy);
        assertNotEquals(swg.getBaseGraph().toString(), swg.toString());
        assertEquals(StringFactory.graphStrategyString(readOnlyGraphStrategy, swg.getBaseGraph()), swg.toString());
    }

    private void assertException(final ThrowingConsumer<Graph> stt) {
        try {
            final StrategyWrappedGraph swg = new StrategyWrappedGraph(g);
            swg.strategy().setGraphStrategy(readOnlyGraphStrategy);
            stt.accept(swg);
            fail();
        } catch (Exception ex) {
            final Exception expectedException = ReadOnlyGraphStrategy.Exceptions.graphUsesReadOnlyStrategy();
            assertEquals(expectedException.getClass(), ex.getClass());
            assertEquals(expectedException.getMessage(), ex.getMessage());

        }
    }
}
