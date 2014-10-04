package com.tinkerpop.gremlin.neo4j;

import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

/**
 * This should only be used for Neo4j-specific testing that is not related to the Gremlin test suite.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BaseNeo4jGraphTest {
    protected Configuration conf;
    protected final DefaultNeo4jGraphProvider graphProvider = new DefaultNeo4jGraphProvider();
    protected Neo4jGraph g;

    @Rule
    public TestName name = new TestName();

    @Before
    public void before() throws Exception {
        // tests that involve legacy indices need legacy indices turned on at startup of the graph.
        final Map<String, Object> neo4jSettings = new HashMap<>();
        if (name.getMethodName().contains("NoMultiProperties"))
            neo4jSettings.put(Neo4jGraph.CONFIG_MULTI_PROPERTIES, false);
        if (name.getMethodName().contains("NoMetaProperties"))
            neo4jSettings.put(Neo4jGraph.CONFIG_META_PROPERTIES, false);
        if (name.getMethodName().contains("Legacy")) {
            neo4jSettings.put("gremlin.neo4j.conf.node_auto_indexing", "true");
            neo4jSettings.put("gremlin.neo4j.conf.relationship_auto_indexing", "true");
        }

        this.conf = neo4jSettings.size() == 0 ?
                this.graphProvider.newGraphConfiguration("standard", this.getClass(), name.getMethodName()) :
                this.graphProvider.newGraphConfiguration("standard", this.getClass(), name.getMethodName(), neo4jSettings);

        this.graphProvider.clear(this.conf);
        this.g = Neo4jGraph.open(this.conf);

    }

    @After
    public void after() throws Exception {
        this.graphProvider.clear(this.g, this.conf);
    }

    protected void tryCommit(final Neo4jGraph g, final Consumer<Neo4jGraph> assertFunction) {
        assertFunction.accept(g);
        if (g.features().graph().supportsTransactions()) {
            g.tx().commit();
            assertFunction.accept(g);
        }
    }

    protected static int countIterable(final Iterable iterable) {
        int count = 0;
        for (Object object : iterable) {
            count++;
        }
        return count;
    }

    protected static void validateCounts(final Neo4jGraph graph, int gV, int gE, int gN, int gR) {
        assertEquals(gV, graph.V().count().next().intValue());
        assertEquals(gE, graph.E().count().next().intValue());
        assertEquals(gN, countIterable(GlobalGraphOperations.at(graph.getBaseGraph()).getAllNodes()));
        assertEquals(gR, countIterable(GlobalGraphOperations.at(graph.getBaseGraph()).getAllRelationships()));
    }
}
