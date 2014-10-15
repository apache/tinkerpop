package com.tinkerpop.gremlin.groovy.jsr223;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptException;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineIntegrateTest extends AbstractGremlinTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotBlowTheHeapParameterized() throws ScriptException {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

        final String[] gremlins = new String[]{
                "g.v(xxx).out().toList()",
                "g.v(xxx).in().toList()",
                "g.v(xxx).out().out().out().toList()",
                "g.v(xxx).out().groupCount()"
        };

        long parameterizedStartTime = System.currentTimeMillis();
        System.out.println("Try to blow the heap with parameterized Gremlin.");
        try {
            for (int ix = 0; ix < 50001; ix++) {
                final Bindings bindings = engine.createBindings();
                bindings.put("g", g);
                bindings.put("xxx", (ix % 4) + 1);
                engine.eval(gremlins[ix % 4], bindings);

                if (ix > 0 && ix % 5000 == 0) {
                    System.out.println(String.format("%s scripts processed in %s (ms) - rate %s (ms/q).", ix, System.currentTimeMillis() - parameterizedStartTime, Double.valueOf(System.currentTimeMillis() - parameterizedStartTime) / Double.valueOf(ix)));
                }
            }
        } catch (OutOfMemoryError oome) {
            fail("Blew the heap - the cache should prevent this from happening.");
        }
    }

    @Test
    public void shouldNotBlowTheHeapUnparameterized() throws ScriptException {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        long notParameterizedStartTime = System.currentTimeMillis();
        System.out.println("Try to blow the heap with non-parameterized Gremlin.");
        try {
            for (int ix = 0; ix < 15001; ix++) {
                final Bindings bindings = engine.createBindings();
                engine.eval(String.format("1+%s", ix), bindings);
                if (ix > 0 && ix % 5000 == 0) {
                    System.out.println(String.format("%s scripts processed in %s (ms) - rate %s (ms/q).", ix, System.currentTimeMillis() - notParameterizedStartTime, Double.valueOf(System.currentTimeMillis() - notParameterizedStartTime) / Double.valueOf(ix)));
                }
            }
        } catch (OutOfMemoryError oome) {
            fail("Blew the heap - the cache should prevent this from happening.");
        }
    }

}
