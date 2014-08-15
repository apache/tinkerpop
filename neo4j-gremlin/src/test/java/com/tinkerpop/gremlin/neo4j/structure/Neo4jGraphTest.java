package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import com.tinkerpop.gremlin.neo4j.Neo4jGraphProvider;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.Schema;

import javax.script.Bindings;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
        final Iterator<Map<String, Object>> result = g.cypher("MATCH (a {name:\"marko\"}) RETURN a", null);
        assertNotNull(result);
        assertTrue(result.hasNext());
    }

    @Test
    public void shouldExecuteCypherWithArgs() throws Exception {
        this.g.addVertex("name", "marko");
        this.g.tx().commit();
        final Map<String,Object> bindings = new HashMap<>();
        bindings.put("n", "marko");
        final Iterator<Map<String, Object>> result = g.cypher("MATCH (a {name:{n}}) RETURN a", bindings);
        assertNotNull(result);
        assertTrue(result.hasNext());
    }

    @Test
    public void shouldExecuteCypherWithArgsUsingVertexIdList() throws Exception {
        final Vertex v = this.g.addVertex("name", "marko");
        final List<Object> idList = Arrays.asList(v.id());
        this.g.tx().commit();

        final Map<String,Object> bindings = new HashMap<>();
        bindings.put("ids", idList);
        final Iterator<String> result = g.cypher("START n=node({ids}) RETURN n", bindings).select("n").value("name");
        assertNotNull(result);
        assertTrue(result.hasNext());
        assertEquals("marko", result.next());
    }

    @Test
    public void shouldExecuteCypherAndBackToGremlin() throws Exception {
        this.g.addVertex("name", "marko", "age", 29, "color", "red");
        this.g.addVertex("name", "marko", "age", 30, "color", "yellow");

        this.g.tx().commit();
        final Traversal result = g.cypher("MATCH (a {name:\"marko\"}) RETURN a").select("a").has("age", 29).value("color");
        assertNotNull(result);
        assertTrue(result.hasNext());
        assertEquals("red", result.next().toString());
    }

    @Test
    public void shouldExecuteMultiIdWhereCypher() throws Exception {
        this.g.addVertex("name", "marko", "age", 29, "color", "red");
        this.g.addVertex("name", "marko", "age", 30, "color", "yellow");
        this.g.addVertex("name", "marko", "age", 30, "color", "orange");
        this.g.tx().commit();

        final List<Object> result = g.cypher("MATCH n WHERE id(n) IN [1,2] RETURN n").select("n").id().toList();
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains(1l));
        assertTrue(result.contains(2l));
    }

    @Test
    public void shouldExecuteMultiIdWhereWithParamCypher() throws Exception {
        final Vertex v1 = this.g.addVertex("name", "marko", "age", 29, "color", "red");
        final Vertex v2 = this.g.addVertex("name", "marko", "age", 30, "color", "yellow");
        this.g.addVertex("name", "marko", "age", 30, "color", "orange");
        this.g.tx().commit();

        final List<Object> ids = Arrays.asList(v1.id(),v2.id());
        final Map<String, Object> m = new HashMap<>();
        m.put("ids", ids);
        final List<Object> result = g.cypher("MATCH n WHERE id(n) IN {ids} RETURN n", m).select("n").id().toList();
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains(v1.id()));
        assertTrue(result.contains(v2.id()));
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
        this.g.tx().readWrite();
        final Schema schema = this.g.getBaseGraph().schema();
        schema.indexFor(DynamicLabel.label("Person")).on("name").create();
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals(2, this.g.V().has(Element.LABEL, "Person").has("name", "marko").count().next(), 0);
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testColonedKeyIsTreatedAsNormalKey() {
        this.g.tx().readWrite();
        final Schema schema = this.g.getBaseGraph().schema();
        schema.indexFor(DynamicLabel.label("Person")).on("name").create();
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals(2, this.g.V().has(Element.LABEL, "Person").has("name", "marko").count().next(), 0);
        assertEquals(0, this.g.V().has("Person:name", "marko").count().next(), 0);

    }

    @Test
    public void testLabeledIndexOnVertexWithHasHasHas() {
        this.g.tx().readWrite();
        final Schema schema = this.g.getBaseGraph().schema();
        schema.indexFor(DynamicLabel.label("Person")).on("name").create();
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
        this.g.tx().readWrite();
        final Schema schema = this.g.getBaseGraph().schema();
        schema.indexFor(DynamicLabel.label("Person")).on("name").create();
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertNotEquals(2l, this.g.V().has("Person:name", "marko").count().next().longValue());
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testLegacyIndexOnVertex() {
        g.tx().readWrite();
        final AutoIndexer<Node> nodeAutoIndexer = this.g.getBaseGraph().index().getNodeAutoIndexer();
        nodeAutoIndexer.startAutoIndexingProperty("name");
        this.g.tx().commit();

        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals(0, this.g.V().has("Person:name", "marko").count().next(), 0);
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testLegacyIndexOnEdge() {
        g.tx().readWrite();
        final AutoIndexer<Relationship> relAutoIndexer = this.g.getBaseGraph().index().getRelationshipAutoIndexer();
        relAutoIndexer.startAutoIndexingProperty("weight");
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
        this.g.tx().readWrite();
        final Schema schema = this.g.getBaseGraph().schema();
        schema.constraintFor(DynamicLabel.label("Person")).assertPropertyIsUnique("name").create();
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals("marko", g.V().<Vertex>has(Element.LABEL, "Person").<Vertex>has("name", "marko").next().value("name"));
    }

    @Test
    public void testMultipleUniqueConstraintPass() {
        this.g.tx().readWrite();
        final Schema schema = this.g.getBaseGraph().schema();
        schema.constraintFor(DynamicLabel.label("Person")).assertPropertyIsUnique("name").create();
        schema.constraintFor(DynamicLabel.label("Person")).assertPropertyIsUnique("surname").create();
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
        this.g.tx().readWrite();
        final Schema schema = this.g.getBaseGraph().schema();
        schema.constraintFor(DynamicLabel.label("Person")).assertPropertyIsUnique("name").create();
        schema.constraintFor(DynamicLabel.label("Person")).assertPropertyIsUnique("surname").create();
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

        this.g.tx().readWrite();
        for (ConstraintDefinition cd : schema.getConstraints(DynamicLabel.label("Person"))) {
            cd.drop();
        }

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
        this.g.tx().readWrite();
        final Schema schema = this.g.getBaseGraph().schema();
        schema.constraintFor(DynamicLabel.label("Person")).assertPropertyIsUnique("name").create();
        this.g.tx().commit();
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
        this.g.tx().commit();
        assertEquals("marko", g.V().<Vertex>has(Element.LABEL, "Person").<Vertex>has("name", "marko").next().value("name"));
        this.g.addVertex(Element.LABEL, "Person", "name", "marko");
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
        g.tx().readWrite();

        final Schema schema = g.getBaseGraph().schema();
        schema.indexFor(DynamicLabel.label("Person")).on("name").create();
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
        g.tx().readWrite();

        final Schema schema = g.getBaseGraph().schema();
        schema.indexFor(DynamicLabel.label("Person")).on("name").create();

        final AutoIndexer<Node> nodeAutoIndexer = this.g.getBaseGraph().index().getNodeAutoIndexer();
        nodeAutoIndexer.startAutoIndexingProperty("name");

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
        g.tx().readWrite();

        final Schema schema = g.getBaseGraph().schema();
        schema.indexFor(DynamicLabel.label("Person")).on("name").create();
        schema.indexFor(DynamicLabel.label("Product")).on("name").create();
        schema.indexFor(DynamicLabel.label("Corporate")).on("name").create();

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
}
