package com.tinkerpop.gremlin.neo4j.process.graph.step.map;

import com.tinkerpop.gremlin.neo4j.BaseNeo4jGraphTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jCypherStartTest extends BaseNeo4jGraphTest {
    @Test
    public void shouldExecuteCypher() throws Exception {
        this.g.addVertex("name", "marko");
        this.g.tx().commit();
        final Iterator<Map<String, Object>> result = g.cypher("MATCH (a {name:\"marko\"}) RETURN a", Collections.emptyMap());
        assertNotNull(result);
        assertTrue(result.hasNext());
    }

    @Test
    public void shouldExecuteCypherWithArgs() throws Exception {
        this.g.addVertex("name", "marko");
        this.g.tx().commit();
        final Map<String, Object> bindings = new HashMap<>();
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

        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("ids", idList);
        final Iterator<String> result = g.cypher("START n=node({ids}) RETURN n", bindings).select("n").values("name");
        assertNotNull(result);
        assertTrue(result.hasNext());
        assertEquals("marko", result.next());
    }

    @Test
    public void shouldExecuteCypherAndBackToGremlin() throws Exception {
        this.g.addVertex("name", "marko", "age", 29, "color", "red");
        this.g.addVertex("name", "marko", "age", 30, "color", "yellow");

        this.g.tx().commit();
        final Traversal result = g.cypher("MATCH (a {name:\"marko\"}) RETURN a").select("a").has("age", 29).values("color");
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

        final List<Object> ids = Arrays.asList(v1.id(), v2.id());
        final Map<String, Object> m = new HashMap<>();
        m.put("ids", ids);
        final List<Object> result = g.cypher("MATCH n WHERE id(n) IN {ids} RETURN n", m).select("n").id().toList();
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains(v1.id()));
        assertTrue(result.contains(v2.id()));
    }

}
