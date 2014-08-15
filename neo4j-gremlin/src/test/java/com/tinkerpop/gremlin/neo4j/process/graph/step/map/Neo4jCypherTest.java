package com.tinkerpop.gremlin.neo4j.process.graph.step.map;

import com.tinkerpop.gremlin.neo4j.BaseNeo4jGraphTest;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jCypherTest extends BaseNeo4jGraphTest {

    @Test
    public void shouldExecuteCypherWithVerticesInput() throws Exception {
        this.g.addVertex("name", "marko", "age", 29, "color", "yellow");
        this.g.addVertex("name", "marko", "age", 30, "color", "yellow");
        final Vertex v = this.g.addVertex("name", "marko", "age", 30, "color", "orange");
        this.g.tx().commit();

        final List result = g.cypher("MATCH n WHERE n.age=30 RETURN n").select("n").id().fold()
                .cypher("MATCH n WHERE id(n) IN {start} AND n.color = \"orange\" RETURN n")
                .select("n").id().toList();

        assertEquals(1, result.size());
        assertTrue(result.contains(v.id()));
    }
}
