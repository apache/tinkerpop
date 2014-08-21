package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.groovy.NoImportCustomizerProvider
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
import org.junit.Ignore
import org.junit.Test

import static org.junit.Assert.fail

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class SugarLoaderTest {

    TinkerGraph g = TinkerFactory.createClassic();

    @Test
    public void shouldNotAllowSugar() {
        try {
            g.V
            fail("Without sugar loaded, the traversal should fail");
        } catch (MissingPropertyException e) {
        } catch (Exception e) {
            fail("Should fail with a MissingPropertyException")
        }

        try {
            g.V().out
            fail("Without sugar loaded, the traversal should fail");
        } catch (MissingPropertyException e) {
        } catch (Exception e) {
            fail("Should fail with a MissingPropertyException")
        }

        try {
            g.V().out().name
            fail("Without sugar loaded, the traversal should fail");
        } catch (MissingPropertyException e) {
        } catch (Exception e) {
            fail("Should fail with a MissingPropertyException")
        }

        // conventional
        g.V().out().value('name');
    }

    @Test
    @Ignore
    public void shouldAllowSugar() {
        // TODO: There is weird caching going on... this sucks.
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new NoImportCustomizerProvider());
        engine.eval("com.tinkerpop.gremlin.groovy.loaders.SugarLoader.load()");
        engine.eval("g = com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.createClassic()");
        engine.eval("org.junit.Assert.assertEquals(g.V(), g.V)");
        engine.eval("org.junit.Assert.assertEquals(g.V().out(), g.V.out)");
        engine.eval("org.junit.Assert.assertEquals(g.V().out().value('name'), g.V.out.name)");

    }
}
