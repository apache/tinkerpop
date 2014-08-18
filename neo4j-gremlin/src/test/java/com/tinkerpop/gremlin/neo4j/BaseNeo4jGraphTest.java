package com.tinkerpop.gremlin.neo4j;

import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.util.HashMap;
import java.util.Map;

/**
 * This should only be used for Neo4j-specific testing that is not related to the Gremlin test suite.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BaseNeo4jGraphTest {
    protected Configuration conf;
    protected final Neo4jGraphProvider graphProvider = new Neo4jGraphProvider();
    protected Neo4jGraph g;

    @Rule
    public TestName name = new TestName();

    @Before
    public void before() throws Exception {
        // tests that involve legacy indices need legacy indices turned on at startup of the graph.
        if (name.getMethodName().contains("Legacy")) {
            final Map<String, Object> neo4jSettings = new HashMap<>();
            neo4jSettings.put("gremlin.neo4j.conf.node_auto_indexing", "true");
            neo4jSettings.put("gremlin.neo4j.conf.relationship_auto_indexing", "true");
            this.conf = this.graphProvider.newGraphConfiguration("standard", this.getClass(), name.getMethodName(), neo4jSettings);
        } else
            this.conf = this.graphProvider.newGraphConfiguration("standard", this.getClass(), name.getMethodName());

        this.graphProvider.clear(this.conf);
        this.g = Neo4jGraph.open(this.conf);
    }

    @After
    public void after() throws Exception {
        this.graphProvider.clear(this.g, this.conf);
    }
}
