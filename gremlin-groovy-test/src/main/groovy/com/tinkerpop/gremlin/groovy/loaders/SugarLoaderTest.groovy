package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.AbstractGremlinTest
import com.tinkerpop.gremlin.LoadGraphWith
import com.tinkerpop.gremlin.groovy.util.SugarTestHelper
import com.tinkerpop.gremlin.structure.Graph
import com.tinkerpop.gremlin.structure.Vertex
import org.junit.Test

/*import com.tinkerpop.gremlin.tinkergraph.process.graph.TinkerElementTraversal;
import com.tinkerpop.gremlin.tinkergraph.process.graph.TinkerGraphTraversal
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex*/
import static org.junit.Assert.*

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class SugarLoaderTest extends AbstractGremlinTest {

    @Override
    protected void afterLoadGraphWith(final Graph g) throws Exception {
        SugarTestHelper.clearRegistry(graphProvider)
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldNotAllowSugar() {
        SugarTestHelper.clearRegistry(graphProvider)
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
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldAllowSugar() {
        SugarLoader.load()
        assertEquals(g.V(), g.V)
        assertEquals(g.V().out(), g.V.out)
        assertEquals(g.V().out().values('name'), g.V.out.name)
        assertEquals(g.V(1).out().out().values('name'), g.V(1).out.out.name);
        g.V(1).next().name = 'okram'
        assertEquals('okram', g.V(1).next().name);
        g.V(1).next()['name'] = 'marko a. rodriguez'
        assertEquals(g.V(1).values('name').toSet(), ["okram", "marko a. rodriguez"] as Set);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldUseTraverserCategoryCorrectly() {
        SugarLoader.load()
        g.V.as('a').out.as('x').name.as('b').back('x').has('age').map { [it.a, it.b, it.age] }.forEach {
            // println it;
            assertTrue(it[0] instanceof Vertex)
            assertTrue(it[1] instanceof String)
            assertTrue(it[2] instanceof Integer)
        };
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void performanceTesting() {
        SugarLoader.load()

        // warm up
        clock(5000) { g.V().both.both.both.value("name").iterate() }
        clock(5000) { g.V().both().both().both().name.iterate() }

        println("g.V")
        println "  Java8-style:  " + clock(5000) { g.V() }
        println "  Groovy-style: " + clock(5000) { g.V }
        assertEquals(g.V(), g.V)

        println("\ng.V.outE.inV")
        println "  Java8-style:  " + clock(5000) { g.V().outE().inV() }
        println "  Groovy-style: " + clock(5000) { g.V.outE.inV }
        assertEquals(g.V().outE().inV(), g.V.outE.inV)

        println("\ng.V.outE.inV.outE.inV")
        println "  Java8-style:  " + clock(5000) { g.V().outE().inV().outE().inV() }
        println "  Groovy-style: " + clock(5000) { g.V.outE.inV.outE.inV }
        assertEquals(g.V().outE().inV().outE().inV(), g.V.outE.inV.outE.inV)

        println("\ng.V.name")
        println "  Java8-style:  " + clock(5000) { g.V().values('name') }
        println "  Groovy-style: " + clock(5000) { g.V.name }
        assertEquals(g.V().values('name'), g.V.name)

        println("\ng.V(1).name")
        println "  Java8-style:  " + clock(5000) { g.V(1).values('name') }
        println "  Groovy-style: " + clock(5000) { g.V(1).name }
        assertEquals(g.V(1).values('name'), g.V(1).name)

        /*println("\ng.V(1)['name'] = 'okram'")
        println "  Java8-style:  " + clock(5000) { g.V(1).property('name', 'okram') }
        println "  Groovy-style: " + clock(5000) { g.V(1)['name'] = 'okram' }

        println("\ng.V(1).name = 'okram'")
        println "  Java8-style:  " + clock(5000) { g.V(1).singleProperty('name', 'okram') }
        println "  Groovy-style: " + clock(5000) { g.V(1).name = 'okram' }*/

        println("\ng.V(1).outE")
        println "  Java8-style:  " + clock(5000) { g.V(1).outE() }
        println "  Groovy-style: " + clock(5000) { g.V(1).outE }
        assertEquals(g.V(1).outE().inV(), g.V(1).outE.inV)

        println("\ng.V.as('a').map{[it.a.name, it.name]}")
        println "  Java8-style:  " + clock(5000) {
            g.V().as('a').map { [it.path('a').value('name'), it.get().value('name')] }
        }
        println "  Groovy-style: " + clock(5000) { g.V.as('a').map { [it.a.name, it.name] } }
        assertEquals(g.V().as('a').map { [it.path('a').value('name'), it.get().value('name')] }, g.V.as('a').map {
            [it.path('a').name, it.name]
        })

    }

    def clock = { int loops = 100, Closure closure ->
        (0..1000).forEach { closure.call() } // warmup
        (1..loops).collect {
            def t = System.nanoTime()
            closure.call()
            ((System.nanoTime() - t) * 0.000001)
        }.mean()
    }


}
