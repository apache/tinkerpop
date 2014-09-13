package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.process.SimpleTraverser
import com.tinkerpop.gremlin.process.graph.GraphTraversal
import com.tinkerpop.gremlin.structure.Graph
import com.tinkerpop.gremlin.structure.Vertex
import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.PathTraverser;
import com.tinkerpop.gremlin.process.Traverser
import com.tinkerpop.gremlin.tinkergraph.process.graph.TinkerElementTraversal;
import com.tinkerpop.gremlin.tinkergraph.process.graph.TinkerGraphTraversal
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex
import org.codehaus.groovy.runtime.InvokerHelper
import org.junit.Ignore
import org.junit.Test

import static org.junit.Assert.*

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class SugarLoaderTest {

    @Test
    public void shouldNotAllowSugar() {
        clearRegistry()
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
        clearRegistry()
        SugarLoader.load()
        Graph g = TinkerFactory.createClassic()
        assertEquals(g.V(), g.V)
        assertEquals(g.V().out(), g.V.out)
        assertEquals(g.V().out().value('name'), g.V.out.name)
        assertEquals(g.v(1).out().out().value('name'), g.v(1).out.out.name);
        g.v(1).name = 'okram'
        assertEquals('okram', g.v(1).name);
        g.v(1)['name'] = 'marko a. rodriguez'
        assertEquals(g.v(1).values('name').toSet(), ["okram", "marko a. rodriguez"] as Set);
        clearRegistry()
    }

    @Test
    public void shouldUseTraverserCategoryCorrectly() {
        clearRegistry()
        SugarLoader.load()
        Graph g = TinkerFactory.createClassic()
        g.V.as('a').out.as('x').name.as('b').back('x').has('age').map { [it.a, it.b, it.age] }.forEach {
            // println it;
            assertTrue(it[0] instanceof Vertex)
            assertTrue(it[1] instanceof String)
            assertTrue(it[2] instanceof Integer)
        };
    }

    @Test
    public void performanceTesting() {
        clearRegistry()
        SugarLoader.load();

        Graph g = TinkerFactory.createClassic();
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

        println("\ng.V.name")
        println "  Java8-style:  " + clock(5000) { g.V().value('name') }
        println "  Groovy-style: " + clock(5000) { g.V.name }
        assertEquals(g.V().value('name'), g.V.name)

        println("\ng.v(1).name")
        println "  Java8-style:  " + clock(5000) { g.v(1).value('name') }
        println "  Groovy-style: " + clock(5000) { g.v(1).name }
        assertEquals(g.v(1).value('name'), g.v(1).name)

        println("\ng.v(1)['name'] = 'okram'")
        g = TinkerFactory.createClassic();
        println "  Java8-style:  " + clock(5000) { g.v(1).property('name', 'okram') }
        g = TinkerFactory.createClassic();
        println "  Groovy-style: " + clock(5000) { g.v(1)['name'] = 'okram' }
        g = TinkerFactory.createClassic();

        println("\ng.v(1).name = 'okram'")
        g = TinkerFactory.createClassic();
        println "  Java8-style:  " + clock(5000) { g.v(1).singleProperty('name', 'okram') }
        g = TinkerFactory.createClassic();
        println "  Groovy-style: " + clock(5000) { g.v(1).name = 'okram' }
        g = TinkerFactory.createClassic();

        println("\ng.v(1).outE.inV")
        println "  Java8-style:  " + clock(5000) { g.v(1).outE().inV() }
        println "  Groovy-style: " + clock(5000) { g.v(1).outE.inV }
        assertEquals(g.v(1).outE().inV(), g.v(1).outE.inV)

        println("\ng.V.as('a').map{[it.a.name, it.name]}")
        println "  Java8-style:  " + clock(5000) {
            g.V().as('a').map { [it.get('a').value('name'), it.get().value('name')] }
        }
        println "  Groovy-style: " + clock(5000) { g.V.as('a').map { [it.a.name, it.name] } }
        assertEquals(g.V().as('a').map { [it.get('a').value('name'), it.get().value('name')] }, g.V.as('a').map {
            [it.a.name, it.name]
        })
    }

    private static clearRegistry() {
        // these calls are required to clear the metaclass registry assuming other "sugar" tests execute first
        InvokerHelper.getMetaRegistry().removeMetaClass(TinkerVertex.class)
        InvokerHelper.getMetaRegistry().removeMetaClass(TinkerGraph.class)
        InvokerHelper.getMetaRegistry().removeMetaClass(TinkerGraphTraversal.class)
        InvokerHelper.getMetaRegistry().removeMetaClass(TinkerElementTraversal.class)
        InvokerHelper.getMetaRegistry().removeMetaClass(Graph.class)
        InvokerHelper.getMetaRegistry().removeMetaClass(GraphTraversal.class)
        InvokerHelper.getMetaRegistry().removeMetaClass(PathTraverser.class)
        InvokerHelper.getMetaRegistry().removeMetaClass(SimpleTraverser.class)
        InvokerHelper.getMetaRegistry().removeMetaClass(Traverser.class)
    }

    def clock = { int loops = 100, Closure closure ->
        closure.call() // warmup
        (1..loops).collect {
            def t = System.nanoTime()
            closure.call()
            ((System.nanoTime() - t) * 0.000001)
        }.mean()
    }


}
