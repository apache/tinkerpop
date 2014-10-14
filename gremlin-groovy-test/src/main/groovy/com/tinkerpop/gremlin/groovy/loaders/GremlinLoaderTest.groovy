package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.AbstractGremlinTest
import com.tinkerpop.gremlin.LoadGraphWith
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
}
