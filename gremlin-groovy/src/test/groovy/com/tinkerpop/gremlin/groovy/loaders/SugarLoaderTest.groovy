package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.NoImportCustomizerProvider
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import com.tinkerpop.gremlin.tinkergraph.process.graph.TinkerGraphTraversal
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
import org.codehaus.groovy.runtime.InvokerHelper
import org.junit.Test

import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

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

        engine.eval("g = com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.createClassic()")
        try {
            engine.eval("g.V")
            fail("Without sugar loaded, the traversal should fail");
        } catch (javax.script.ScriptException e) {
            assertTrue(e.getCause().getCause() instanceof MissingPropertyException)
        } catch (Exception e) {
            fail("Should fail with a MissingPropertyException")
        }

        try {
            engine.eval("g.V().out")
            fail("Without sugar loaded, the traversal should fail");
        } catch (javax.script.ScriptException e) {
            assertTrue(e.getCause().getCause() instanceof MissingPropertyException)
        } catch (Exception e) {
            fail("Should fail with a MissingPropertyException")
        }

        try {
            engine.eval("g.V().out().name")
            fail("Without sugar loaded, the traversal should fail");
        } catch (javax.script.ScriptException e) {
            assertTrue(e.getCause().getCause() instanceof MissingPropertyException)
        } catch (Exception e) {
            fail("Should fail with a MissingPropertyException")
        }
    }

    @Test
    public void shouldAllowSugar() {
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new NoImportCustomizerProvider())

        // these calls are required to clear the metaclass registry assuming other "sugar" tests execute first
        InvokerHelper.getMetaRegistry().removeMetaClass(TinkerGraph)
        InvokerHelper.getMetaRegistry().removeMetaClass(TinkerGraphTraversal)
        engine.eval("com.tinkerpop.gremlin.groovy.loaders.SugarLoader.load()")
        engine.eval("g = com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.createClassic()")
        engine.eval("org.junit.Assert.assertEquals(g.V(), g.V)")
        engine.eval("org.junit.Assert.assertEquals(g.V().out(), g.V.out)")
        engine.eval("org.junit.Assert.assertEquals(g.V().out().value('name'), g.V.out.name)")

    }
}
