package com.tinkerpop.gremlin.groovy.util

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import com.tinkerpop.gremlin.groovy.loaders.GremlinLoader
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized.Parameter
import org.junit.runners.Parameterized.Parameters
import org.junit.runners.Parameterized

import javax.script.SimpleBindings

import static org.junit.Assert.assertEquals

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
class GremlinHelperTest {
    private static GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine()
    private static LINE_SEPARATOR = System.getProperty("line.separator")

    @Parameters(name = "{index}: {0}.test()")
    public static Iterable<Object[]> data() {
        return [["simple constant", "'testing'$LINE_SEPARATOR", { 'testing' }, [], []] as Object[],
                ["add constants", "1 + 2$LINE_SEPARATOR", { 1 + 2 }, [], []] as Object[],
                ["sum one variable", "x + 1$LINE_SEPARATOR", {x -> x + 1 }, [2], ['x']] as Object[],
                ["sum multi-variable", "x + y $LINE_SEPARATOR", {x, y -> x + y }, [1,2], ['y','x']] as Object[],
                ["simple multiline with explicit declaration", "java.lang.Integer z = x + y ${LINE_SEPARATOR}z / 2$LINE_SEPARATOR", { x, y ->
                    int z = x + y
                    z / 2
                }, [2,2], ['y','x']] as Object[],
                ["simple multiline with def", "java.lang.Object z = x + y ${LINE_SEPARATOR}z / 2$LINE_SEPARATOR", { x, y ->
                    def z = x + y
                    z / 2
                }, [2,2], ['y','x']] as Object[],
                ["embedded closure", "[ x , y ].findAll({ $LINE_SEPARATOR    it > 1$LINE_SEPARATOR})$LINE_SEPARATOR", {x, y -> [x,y].findAll{it>1} }, [1,2], ['y','x']] as Object[]];
    }

    @Parameter(value = 0)
    public String testName

    @Parameter(value = 1)
    public String expectedBody

    @Parameter(value = 2)
    public Closure closure

    @Parameter(value = 3)
    public List argumentsToClosure

    @Parameter(value = 4)
    public List bindingsNames

    static {
        GremlinLoader.load()
    }

    @Test
    public void shouldReturnValidClosureString() {
        def body = GremlinHelper.getClosureBody(closure)
        def bindings = [:]
        bindingsNames.eachWithIndex{ entry, i -> bindings<<[(entry):argumentsToClosure[i]]}
        assertEquals(expectedBody, body)
        def argCount = closure.getParameterTypes().size()
        if (argCount <= 1)
            assertEquals(argumentsToClosure.size() == 0 ? closure.call() : closure.call(argumentsToClosure[0]), scriptEngine.eval(body, new SimpleBindings(bindings)))
        else
            assertEquals(closure.call(* (argumentsToClosure)), scriptEngine.eval(body, new SimpleBindings(bindings)))
    }
}
