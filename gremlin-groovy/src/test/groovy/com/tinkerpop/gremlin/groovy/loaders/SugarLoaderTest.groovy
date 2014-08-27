package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.NoImportCustomizerProvider
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import com.tinkerpop.gremlin.structure.Graph
import com.tinkerpop.gremlin.tinkergraph.process.graph.TinkerGraphTraversal
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
import org.codehaus.groovy.runtime.InvokerHelper
import org.junit.Test

import static org.junit.Assert.assertEquals
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class SugarLoaderTest {

    @Test
    public void shouldNotAllowSugar() {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new NoImportCustomizerProvider());

        // these calls are required to clear the metaclass registry assuming other "sugar" tests execute first
        InvokerHelper.getMetaRegistry().removeMetaClass(TinkerGraph)
        InvokerHelper.getMetaRegistry().removeMetaClass(TinkerGraphTraversal)

        Graph g = TinkerFactory.createClassic()
        try {
            g.V
            fail("Without sugar loaded, the traversal should fail");
        } catch (MissingPropertyException e) {

        } catch (Exception e) {
            fail("Should fail with a MissingPropertyException: " + e)
        }

        try {
            g.V().out
            fail("Without sugar loaded, the traversal should fail");
        } catch (MissingPropertyException e) {

        } catch (Exception e) {
            fail("Should fail with a MissingPropertyException:" + e)
        }

        try {
            g.V().out().name
            fail("Without sugar loaded, the traversal should fail");
        } catch (MissingPropertyException e) {

        } catch (Exception e) {
            fail("Should fail with a MissingPropertyException: " + e)
        }
    }

    @Test
    public void shouldAllowSugar() {
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new NoImportCustomizerProvider())

        // these calls are required to clear the metaclass registry assuming other "sugar" tests execute first
        InvokerHelper.getMetaRegistry().removeMetaClass(TinkerGraph)
        InvokerHelper.getMetaRegistry().removeMetaClass(TinkerGraphTraversal)
        SugarLoader.load()
        Graph g = TinkerFactory.createClassic()
        assertEquals(g.V(), g.V)
        assertEquals(g.V().out(), g.V.out)
        assertEquals(g.V().out().value('name'), g.V.out.name)

    }
}
