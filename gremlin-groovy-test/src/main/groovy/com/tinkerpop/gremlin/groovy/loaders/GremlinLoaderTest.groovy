package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.AbstractGremlinTest
import com.tinkerpop.gremlin.FeatureRequirement
import com.tinkerpop.gremlin.FeatureRequirements
import com.tinkerpop.gremlin.LoadGraphWith
import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.structure.Graph
import com.tinkerpop.gremlin.structure.IoTest
import org.junit.Test

import static org.junit.Assert.assertEquals

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinLoaderTest extends AbstractGremlinTest {

    @Test
    public void shouldHideAndUnhideKeys() {
        assertEquals(Graph.Key.hide("g"), -"g");
        assertEquals("g", -Graph.Key.hide("g"));
        assertEquals("g", -(-"g"));
    }

    @Test
    @FeatureRequirements([
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS),
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_STRING_IDS),
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES),
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)])
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldSaveLoadGraphML() {
        def graphmlDirString = tempPath + File.separator + "graphml" + File.separator
        def graphmlDir = new File(graphmlDirString)
        graphmlDir.deleteDir()
        graphmlDir.mkdirs()

        def graphml = graphmlDirString + UUID.randomUUID().toString() + ".xml"
        g.saveGraphML(graphml)

        def conf = graphProvider.newGraphConfiguration("g1", GremlinLoaderTest.class, name.methodName)
        def gcopy = graphProvider.openTestGraph(conf)
        gcopy.loadGraphML(graphml)

        IoTest.assertClassicGraph(gcopy, false, true)

        graphProvider.clear(gcopy, conf)
    }

    @Test
    @FeatureRequirements([
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS),
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS),
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES),
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)])
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldSaveLoadGraphSON() {
        def graphsonDirString = tempPath + File.separator + "graphson" + File.separator
        def graphsonDir = new File(graphsonDirString)
        graphsonDir.deleteDir()
        graphsonDir.mkdirs()

        def graphson = graphsonDirString + UUID.randomUUID().toString() + ".json"
        g.saveGraphSON(graphson)

        def conf = graphProvider.newGraphConfiguration("g1", GremlinLoaderTest.class, name.methodName)
        def gcopy = graphProvider.openTestGraph(conf)
        gcopy.loadGraphSON(graphson)

        IoTest.assertClassicGraph(gcopy, true, true)

        graphProvider.clear(gcopy, conf)
    }

    @Test
    @FeatureRequirements([
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS),
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_NUMERIC_IDS),
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES),
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)])
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldSaveLoadKryo() {
        def kryoDirString = tempPath + File.separator + "kryo" + File.separator
        def kryoDir = new File(kryoDirString)
        kryoDir.deleteDir()
        kryoDir.mkdirs()

        def kryo = kryoDirString + UUID.randomUUID().toString() + ".json"
        g.saveKryo(kryo)

        def conf = graphProvider.newGraphConfiguration("g1", GremlinLoaderTest.class, name.methodName)
        def gcopy = graphProvider.openTestGraph(conf)
        gcopy.loadKryo(kryo)

        IoTest.assertCrewGraph(gcopy, true)

        graphProvider.clear(gcopy, conf)
    }

    @Test
    public void shouldCalculateMean() {
        assertEquals(25.0d, [10, 20, 30, 40].mean(), 0.00001d)
    }
}
