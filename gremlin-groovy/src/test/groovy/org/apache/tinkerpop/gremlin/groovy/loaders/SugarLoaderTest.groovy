/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.groovy.loaders

import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine
import org.apache.tinkerpop.gremlin.groovy.jsr223.SugarGremlinPlugin
import org.apache.tinkerpop.gremlin.groovy.util.SugarTestHelper
import org.apache.tinkerpop.gremlin.jsr223.ScriptCustomizer
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.structure.*
import org.apache.tinkerpop.gremlin.structure.util.StringFactory
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
import org.junit.After
import org.junit.Before
import org.junit.Test

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt
import static org.junit.Assert.*

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class SugarLoaderTest {

    @Before
    public void setup() throws Exception {
        SugarTestHelper.clearRegistry()
    }

    @Test
    public void shouldNotAllowSugar() {
        def graph = TinkerFactory.createModern()
        def g = graph.traversal()
        SugarTestHelper.clearRegistry()
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
        def graph = TinkerFactory.createModern()
        def g = graph.traversal()
        SugarLoader.load()
        assertEquals(6, g.V.count.next())
        assertEquals(6, g.V.out.count.next())
        assertEquals(6, g.V.out.name.count.next())
        assertEquals(2, g.V(convertToVertexId(graph, "marko")).out.out.name.count.next());
        final Object markoId = convertToVertexId(graph, "marko");
        g.V(markoId).next().name = 'okram'
        assertEquals('okram', g.V(markoId).next().name);
        assertEquals(29, g.V.age.is(eq(29)).next())
        if (graph.features().vertex().supportsMultiProperties()) {
            g.V(markoId).next()['name'] = 'marko a. rodriguez'
            assertEquals(["okram", "marko a. rodriguez"] as Set, g.V(markoId).values('name').toSet());
        }
    }

    @Test
    public void shouldUseTraverserCategoryCorrectly() {
        def graph = TinkerFactory.createModern()
        def g = graph.traversal()
        SugarLoader.load()
        final Traversal t = g.V.as('a').out.as('x').name.as('b').select('x').has('age').map {
            [it.path().a, it.path().b, it.age]
        };
        assertTrue(t.hasNext())
        t.forEachRemaining {
            assertTrue(it[0] instanceof Vertex)
            assertTrue(it[1] instanceof String)
            assertTrue(it[2] instanceof Integer)
        };
    }

    @Test
    public void shouldHaveProperToStringOfMixins() {
        def graph = TinkerFactory.createModern()
        def g = graph.traversal()
        SugarLoader.load();
        final Vertex vertex = graph.vertices().next();
        final Edge edge = graph.edges().next();
        final VertexProperty vertexProperty = vertex.property('name');
        final Property property = edge.property('weight');

        assertEquals(StringFactory.vertexString(vertex), vertex.toString());
        assertEquals(StringFactory.edgeString(edge), edge.toString());
        assertEquals(StringFactory.propertyString(vertexProperty), vertexProperty.toString());
        assertEquals(StringFactory.propertyString(property), property.toString());
        assertEquals(StringFactory.traversalSourceString(g), g.toString());
        //assertEquals(StringFactory.traversalSourceString(g.withPath()), g.withPath().toString());
        assertEquals(StringFactory.traversalString(g.V().out().asAdmin()), g.V().out().toString());
        assertEquals(StringFactory.traversalString(g.V.out), g.V.out.toString());
        assertEquals(convertToVertex(graph, "marko").toString(), g.V(convertToVertexId(graph, "marko")).next().toString())
    }

    @After
    public void tearDown() throws Exception {
        // sugar changes global metaclasses, so turn it off again for other tests in the same JVM
        SugarTestHelper.clearRegistry()
    }

    @Test
    public void shouldAllowRangeAndIndexSubscripts() {
        def g = TinkerFactory.createModern().traversal()
        SugarLoader.load()
        // [a..b] is inclusive in Groovy but becomes range(a, b), which excludes b
        assertEquals(2L, g.V[0..2].count().next())
        assertEquals(['marko', 'vadas'], g.V[0..2].name.toList())
        assertEquals(['vadas'], g.V[1].name.toList())
    }

    @Test
    public void shouldAllowOrAndOperators() {
        def g = TinkerFactory.createModern().traversal()
        SugarLoader.load()
        assertEquals(['josh', 'peter'], g.V.where(__.has('age', gt(31)) | __.has('name', 'josh')).name.toList())
        assertEquals(['josh'], g.V.where(__.has('age', gt(30)) & __.has('name', 'josh')).name.toList())
    }

    @Test
    public void shouldAllowAnonymousTraversalSugar() {
        def g = TinkerFactory.createModern().traversal()
        SugarLoader.load()
        assertEquals(['lop', 'vadas', 'josh'], g.V(1).flatMap(__.out).name.toList())
        assertEquals(['marko'], g.V(1).map(__.name).toList())
        assertEquals([29], g.V(1).map(__.age).toList())
    }

    @Test
    public void shouldAllowEdgesAndTraverserMethodDelegation() {
        def g = TinkerFactory.createModern().traversal()
        SugarLoader.load()
        assertEquals(6L, g.E.count.next())
        // methods missing on the Traverser are called on the object it holds
        assertEquals(['person', 'person', 'software', 'person', 'software', 'person'], g.V.map { it.label() }.toList())
        assertEquals(['marko', 'vadas'], g.V.limit(2).map { it.name }.toList())
    }

    @Test
    public void shouldKeepStepLoaderAdditionsWhenSugarIsLoaded() {
        def g = TinkerFactory.createModern().traversal()
        GremlinLoader.load()
        SugarLoader.load()
        assertEquals(6L, g.withStrategies(org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy).V().count().next())
        assertEquals([null], g.inject(null).toList())
    }

    @Test
    public void shouldAllowMapRangeSubscript() {
        SugarLoader.load()
        assertEquals([a: 1, b: 2], [a: 1, b: 2, c: 3][0..1])
        assertEquals([b: 2, c: 3], [a: 1, b: 2, c: 3][1..5])
    }

    @Test
    public void shouldAllowSugarThroughTheScriptEngine() {
        // the engine does not run plugin scripts itself; like the script engine manager in Gremlin Server, evaluate
        // the plugin's script customizer (which calls SugarLoader.load()) before using the sugar
        def engine = new GremlinGroovyScriptEngine()
        new SugarGremlinPlugin().getCustomizers("gremlin-groovy").get()
                .findAll { it instanceof ScriptCustomizer }
                .each { ((ScriptCustomizer) it).getScripts().each { lines -> engine.eval(lines.join("\n")) } }
        def bindings = engine.createBindings()
        bindings.put("g", TinkerFactory.createModern().traversal())
        assertEquals(6L, engine.eval("g.V.count.next()", bindings))
        assertEquals(['marko', 'vadas'], engine.eval("g.V[0..2].name.toList()", bindings))
        assertEquals(['lop', 'vadas', 'josh'], engine.eval("g.V(1).out.name.toList()", bindings))
        assertEquals(['josh', 'peter'], engine.eval("g.V.where(__.has('age', gt(31)) | __.has('name', 'josh')).name.toList()", bindings))
        assertEquals(['person', 'person'], engine.eval("g.V.limit(2).map{ it.label() }.toList()", bindings))
    }

    private Object convertToVertexId(final Graph graph, final String vertexName) {
        return convertToVertex(graph, vertexName).id();
    }

    private Vertex convertToVertex(final Graph graph, final String vertexName) {
        // all test graphs have "name" as a unique id which makes it easy to hardcode this...works for now
        return graph.traversal().V().has("name", vertexName).next();
    }
}
