package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import com.tinkerpop.gremlin.neo4j.Neo4jGraphProvider;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexDefinition;

import javax.script.Bindings;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.junit.Assert.*;

/**
 * These are tests specific to Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Pieter Martin
 */
public class Neo4jGraphTest {

    private Configuration conf;
    private final Neo4jGraphProvider graphProvider = new Neo4jGraphProvider();
    private Neo4jGraph g;

    @Rule
    public TestName name = new TestName();

    @Before
    public void before() throws Exception {
        // tests that involve legacy indices need legacy indices turned on at startup of the graph.
        if (name.getMethodName().contains("Legacy")) {
            final Map<String,Object> neo4jSettings = new HashMap<>();
            neo4jSettings.put("gremlin.neo4j.conf.node_auto_indexing", "true");
            neo4jSettings.put("gremlin.neo4j.conf.relationship_auto_indexing", "true");
            this.conf = this.graphProvider.newGraphConfiguration("standard", neo4jSettings);
        } else
            this.conf = this.graphProvider.newGraphConfiguration("standard");

        this.graphProvider.clear(this.conf);
        this.g = Neo4jGraph.open(this.conf);
    }

    @After
    public void after() throws Exception {
        this.graphProvider.clear(this.g, this.conf);
    }

    @Test
    public void shouldOpenWithOverriddenConfig() throws Exception {
        assertNotNull(this.g);
    }

    @Test
    public void shouldExecuteCypher() throws Exception {
        this.g.addVertex("name", "marko");
        this.g.tx().commit();
        final Iterator<Map<String, Object>> result = g.query("MATCH (a {name:\"marko\"}) RETURN a", null);
        assertNotNull(result);
        assertTrue(result.hasNext());
    }

    @Test
    public void testNoConcurrentModificationException() {
        this.g.addVertex("name", "a");
        this.g.addVertex("name", "b");
        this.g.addVertex("name", "c");
        this.g.addVertex("name", "d");
        this.g.V().forEach(Vertex::remove);
        this.g.tx().commit();
        assertEquals(0, this.g.V().count().next(), 0);
    }

    @Test
    public void testLabeledIndexOnVertexWithHasHas() {
        this.g.createLabeledIndex("Person", "name");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals(2, this.g.V().has(Element.LABEL, "Person").has("name", "marko").count().next(), 0);
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testLabeledIndexWithTimeoutOnVertexWithHasHas() {
        IndexDefinition index = this.g.createLabeledIndex("Person", "name");
        this.g.tx().commit();
        this.g.awaitIndexOnline(index,  10, TimeUnit.SECONDS);
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals(2, this.g.V().has(Element.LABEL, "Person").has("name", "marko").count().next(), 0);
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testColonedKeyIsTreatedAsNormalKey() {
        this.g.createLabeledIndex("Person", "name");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals(2, this.g.V().has(Element.LABEL, "Person").has("name", "marko").count().next(), 0);
        assertEquals(0, this.g.V().has("Person:name", "marko").count().next(), 0);

    }

    @Test
    public void testLabeledIndexOnVertexWithHasHasHas() {
        this.g.createLabeledIndex("Person", "name");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko", "color", "blue");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko", "color", "green");
        this.g.tx().commit();
        assertEquals(1, this.g.V().has(Element.LABEL, "Person").has("name", "marko").has("color", "blue").count().next(), 0);
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testVertexWithHasHasHasNoIndex() {
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko", "color", "blue");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko", "color", "green");
        this.g.tx().commit();
        assertEquals(1, this.g.V().has(Element.LABEL, "Person").has("name", "marko").has("color", "blue").count().next(), 0);
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testLabeledIndexOnVertexWithColonFails() {
        this.g.createLabeledIndex("Person", "name");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertNotEquals(2l, this.g.V().has("Person:name", "marko").count().next().longValue());
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testLegacyIndexOnVertex() {
        this.g.createLegacyIndex(Vertex.class, "name");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals(0, this.g.V().has("Person:name", "marko").count().next(), 0);
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testLegacyIndexOnEdge() {
        this.g.createLegacyIndex(Edge.class, "weight");
        this.g.tx().commit();
        Vertex marko = this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex john = this.g.addVertex(Element.LABEL, "Person", "name", "john");
        Vertex pete = this.g.addVertex(Element.LABEL, "Person", "name", "pete");
        marko.addEdge("friend", john, "weight", "a");
        marko.addEdge("friend", pete, "weight", "a");
        this.g.tx().commit();
        assertEquals(2, this.g.E().has("weight", "a").count().next(), 0);
    }

    @Test
    public void testUniqueConstraintPass() {
        this.g.createUniqueConstraint("Person", "name");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals("marko", g.V().<Vertex>has(Element.LABEL, "Person").<Vertex>has("name", "marko").next().value("name"));
    }

    @Test
    public void testMultipleUniqueConstraintPass() {
        this.g.createUniqueConstraint("Person", "name");
        this.g.createUniqueConstraint("Person", "surname");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "surname", "aaaa");
        this.g.tx().commit();
        boolean failSurname = false;
        try {
            this.g.addVertex(Element.LABEL, "Person", "surname", "aaaa");
        } catch (ConstraintViolationException e) {
            failSurname = true;
        }
        assertTrue(failSurname);
        boolean failName = false;
        try {
            this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        } catch (ConstraintViolationException e) {
            failName = true;
        }
        assertTrue(failName);
        this.g.tx().commit();
    }

    @Test
    public void testDropMultipleUniqueConstraintPass() {
        this.g.createUniqueConstraint("Person", "name");
        this.g.createUniqueConstraint("Person", "surname");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "surname", "aaaa");
        this.g.tx().commit();
        boolean failSurname = false;
        try {
            this.g.addVertex(Element.LABEL, "Person", "surname", "aaaa");
        } catch (ConstraintViolationException e) {
            failSurname = true;
        }
        assertTrue(failSurname);
        boolean failName = false;
        try {
            this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        } catch (ConstraintViolationException e) {
            failName = true;
        }
        assertTrue(failName);
        this.g.tx().commit();
        this.g.dropConstraint("Person");
        this.g.tx().commit();
        assertEquals(1, this.g.V().has(Element.LABEL, "Person").<Vertex>has("name", "marko").count().next(), 0);
        assertEquals(1, this.g.V().has(Element.LABEL, "Person").<Vertex>has("surname", "aaaa").count().next(), 0);
        this.g.addVertex(Element.LABEL, "Person", "surname", "aaaa");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals(2, this.g.V().has(Element.LABEL, "Person").<Vertex>has("name", "marko").count().next(), 0);
        assertEquals(2, this.g.V().has(Element.LABEL, "Person").<Vertex>has("surname", "aaaa").count().next(), 0);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testUniqueConstraintFail() {
        this.g.createUniqueConstraint("Person", "name");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals("marko", g.V().<Vertex>has(Element.LABEL, "Person").<Vertex>has("name", "marko").next().value("name"));
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
    }

    @Test
    public void testDropLabeledIndex() {
        this.g.createLabeledIndex("Person", "name");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals(2, this.g.V().has(Element.LABEL, "Person").has("name", "marko").count().next(), 0);
        this.g.dropLabeledIndex("Person");
        this.g.tx().commit();
        assertEquals(0, this.g.V().has("Person:name", "marko").count().next(), 0);
        assertEquals(2, this.g.V().has(Element.LABEL, "Person").has("name", "marko").count().next(), 0);
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testDropLegacyIndex() {
        this.g.createLegacyIndex(Vertex.class, "name");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals(0, this.g.V().has("Person:name", "marko").count().next(), 0);
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
        this.g.dropLegacyIndex(Vertex.class, "name");
        this.g.tx().commit();
        assertEquals(0, this.g.V().has("Person:name", "marko").count().next(), 0);
        assertEquals(2, this.g.V().has(Element.LABEL, "Person").has("name", "marko").count().next(), 0);
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testTraverseRelationshipNeedsTx() throws ScriptException {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("#jsr223.groovy.engine.keep.globals", "phantom");

        Vertex marko = this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex john = this.g.addVertex(Element.LABEL, "Person", "name", "john");
        Vertex pete = this.g.addVertex(Element.LABEL, "Person", "name", "pete");
        marko.addEdge("friend", john);
        marko.addEdge("friend", pete);
        this.g.tx().commit();

        Object result = engine.eval("g.v(" + marko.id().toString() + ").outE('friend')", bindings);
        assertTrue(result instanceof GraphTraversal);

        this.g.tx().commit();
        assertEquals(2L, ((GraphTraversal) result).count().next());
    }

    @Test
    public void testTraverseVertexesNeedsTx() throws ScriptException {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("#jsr223.groovy.engine.keep.globals", "phantom");

        Vertex marko = this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex john = this.g.addVertex(Element.LABEL, "Person", "name", "john");
        Vertex pete = this.g.addVertex(Element.LABEL, "Person", "name", "pete");
        marko.addEdge("friend", john);
        marko.addEdge("friend", pete);
        this.g.tx().commit();

        Object result = engine.eval("g.v(" + marko.id().toString() + ").out('friend')", bindings);
        assertTrue(result instanceof GraphTraversal);

        this.g.tx().commit();
        assertEquals(2L, ((GraphTraversal) result).count().next());
    }

    @Test
    public void testLabelSearch() {
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "john");
        Vertex pete = this.g.addVertex(Element.LABEL, "Person", "name", "pete");
        this.g.addVertex(Element.LABEL, "Monkey", "name", "pete");
        this.g.tx().commit();
        assertEquals(3, this.g.V().has(Element.LABEL, "Person").count().next(), 0);
        pete.remove();
        this.g.tx().commit();
        assertEquals(2, this.g.V().has(Element.LABEL, "Person").count().next(), 0);
    }

    @Test
    public void testLabelAndIndexSearch() {
        this.g.createLabeledIndex("Person", "name");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "john");
        this.g.addVertex(Element.LABEL, "Person", "name", "pete");
        this.g.tx().commit();
        assertEquals(0, this.g.V().has("Person:name", "marko").count().next(), 0);
        assertEquals(3, this.g.V().has(Element.LABEL, "Person").count().next(), 0);
        assertEquals(1, this.g.V().has(Element.LABEL, "Person").has("name", "marko").count().next(), 0);
    }

    @Test
    public void testLabelAndLegacyIndexSearch() {
        this.g.createLabeledIndex("Person", "name");
        this.g.createLegacyIndex(Vertex.class, "name");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "john");
        this.g.addVertex(Element.LABEL, "Person", "name", "pete");
        this.g.tx().commit();
        assertEquals(1, this.g.V().has(Element.LABEL, "Person").has("name", "marko").count().next(), 0);
        assertEquals(3, this.g.V().has(Element.LABEL, "Person").count().next(), 0);
        assertEquals(1, this.g.V().has("name", "john").count().next(), 0);

    }

    @Test
    public void testLabelsNameSpaceBehavior() {
        this.g.createLabeledIndex("Person", "name");
        this.g.createLabeledIndex("Product", "name");
        this.g.createLabeledIndex("Corporate", "name");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "john");
        this.g.addVertex(Element.LABEL, "Person", "name", "pete");
        this.g.addVertex(Element.LABEL, "Product", "name", "marko");
        this.g.addVertex(Element.LABEL, "Product", "name", "john");
        this.g.addVertex(Element.LABEL, "Product", "name", "pete");
        this.g.addVertex(Element.LABEL, "Corporate", "name", "marko");
        this.g.addVertex(Element.LABEL, "Corporate", "name", "john");
        this.g.addVertex(Element.LABEL, "Corporate", "name", "pete");
        this.g.tx().commit();
        assertEquals(1, this.g.V().has(Element.LABEL, "Person").has("name", "marko").has(Element.LABEL, "Person").count().next(), 0);
        assertEquals(1, this.g.V().has(Element.LABEL, "Product").has("name", "marko").has(Element.LABEL, "Product").count().next(), 0);
        assertEquals(1, this.g.V().has(Element.LABEL, "Corporate").has("name", "marko").has(Element.LABEL, "Corporate").count().next(), 0);
        assertEquals(0, this.g.V().has(Element.LABEL, "Person").has("name", "marko").has(Element.LABEL, "Product").count().next(), 0);
        assertEquals(0, this.g.V().has(Element.LABEL, "Product").has("name", "marko").has(Element.LABEL, "Person").count().next(), 0);
        assertEquals(0, this.g.V().has(Element.LABEL, "Corporate").has("name", "marko").has(Element.LABEL, "Person").count().next(), 0);
    }

    @Test
    public void testGetIndexes() {
        this.g.createLabeledIndex("Person", "name");
        this.g.createLabeledIndex("Product", "name");
        this.g.createLabeledIndex("Corporate", "name");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Product", "name", "car");
        this.g.addVertex(Element.LABEL, "Corporate", "name", "google");
        this.g.tx().commit();
        assertEquals(3, StreamSupport.stream(this.g.getLabeledIndexes().spliterator(), false).count());
    }

    @Test
    public void getLegacyIndexKeys()  {
        this.g.createLegacyIndex(Vertex.class, "name1");
        this.g.createLegacyIndex(Vertex.class, "name2");
        this.g.createLegacyIndex(Vertex.class, "name3");
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name1", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name2", "john");
        this.g.addVertex(Element.LABEL, "Person", "name3", "pete");
        this.g.tx().commit();
        assertEquals(3, this.g.getIndexedKeys(Vertex.class).size());
        assertTrue(this.g.getIndexedKeys(Vertex.class).contains("name1"));
        assertTrue(this.g.getIndexedKeys(Vertex.class).contains("name2"));
        assertTrue(this.g.getIndexedKeys(Vertex.class).contains("name3"));
    }

}
