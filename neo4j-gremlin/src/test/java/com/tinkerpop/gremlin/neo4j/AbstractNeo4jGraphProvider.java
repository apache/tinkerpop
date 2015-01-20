package com.tinkerpop.gremlin.neo4j;

import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jEdgeTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jElementTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jGraphTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jVertexPropertyTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jVertexTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.util.DefaultNeo4jGraphTraversal;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jElement;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraphVariables;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jProperty;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jVertexProperty;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.neo4j.graphdb.DynamicLabel;

import java.io.File;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractNeo4jGraphProvider extends AbstractGraphProvider {
    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
        add(Neo4jEdge.class);
        add(Neo4jElement.class);
        add(Neo4jEdgeTraversal.class);
        add(Neo4jElementTraversal.class);
        add(Neo4jGraphTraversal.class);
        add(Neo4jVertexPropertyTraversal.class);
        add(DefaultNeo4jGraphTraversal.class);
        add(Neo4jVertexTraversal.class);
        add(Neo4jGraph.class);
        add(Neo4jGraphVariables.class);
        add(Neo4jProperty.class);
        add(Neo4jVertex.class);
        add(Neo4jVertexProperty.class);
    }};

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (null != g) {
            if (g.features().graph().supportsTransactions() && g.tx().isOpen())
                g.tx().rollback();
            g.close();
        }

        if (configuration.containsKey("gremlin.neo4j.directory")) {
            // this is a non-in-sideEffects configuration so blow away the directory
            final File graphDirectory = new File(configuration.getString("gremlin.neo4j.directory"));
            deleteDirectory(graphDirectory);
        }
    }

    @Override
    public void loadGraphData(final Graph g, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        if (loadGraphWith != null) this.createIndices((Neo4jGraph) g, loadGraphWith.value());
        super.loadGraphData(g, loadGraphWith, testClass, testName);
    }

    private void createIndices(final Neo4jGraph g, final LoadGraphWith.GraphData graphData) {
        final Random random = new Random();
        final int pick = random.nextInt(3);
        //final int pick = 2;
        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            if (pick == 1) {
                g.tx().readWrite();
                if (random.nextBoolean())
                    g.getBaseGraph().schema().indexFor(DynamicLabel.label("artist")).on("name").create();
                if (random.nextBoolean())
                    g.getBaseGraph().schema().indexFor(DynamicLabel.label("song")).on("name").create();
                if (random.nextBoolean())
                    g.getBaseGraph().schema().indexFor(DynamicLabel.label("song")).on("songType").create();
                if (random.nextBoolean())
                    g.getBaseGraph().schema().indexFor(DynamicLabel.label("song")).on("performances").create();
                g.tx().commit();
            } else if (pick == 2) {
                g.tx().readWrite();
                g.getBaseGraph().index().getNodeAutoIndexer().setEnabled(true);
                if (random.nextBoolean())
                    g.getBaseGraph().index().getNodeAutoIndexer().startAutoIndexingProperty("name");
                if (random.nextBoolean())
                    g.getBaseGraph().index().getNodeAutoIndexer().startAutoIndexingProperty("songType");
                if (random.nextBoolean())
                    g.getBaseGraph().index().getNodeAutoIndexer().startAutoIndexingProperty("performances");
                g.tx().commit();
            }
        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            if (pick == 1) {
                g.tx().readWrite();
                if (random.nextBoolean())
                    g.getBaseGraph().schema().indexFor(DynamicLabel.label("person")).on("name").create();
                if (random.nextBoolean())
                    g.getBaseGraph().schema().indexFor(DynamicLabel.label("person")).on("age").create();
                if (random.nextBoolean())
                    g.getBaseGraph().schema().indexFor(DynamicLabel.label("software")).on("name").create();
                if (random.nextBoolean())
                    g.getBaseGraph().schema().indexFor(DynamicLabel.label("software")).on("lang").create();
                g.tx().commit();
            } else if (pick == 2) {
                g.tx().readWrite();
                g.getBaseGraph().index().getNodeAutoIndexer().setEnabled(true);
                if (random.nextBoolean())
                    g.getBaseGraph().index().getNodeAutoIndexer().startAutoIndexingProperty("name");
                if (random.nextBoolean())
                    g.getBaseGraph().index().getNodeAutoIndexer().startAutoIndexingProperty("age");
                if (random.nextBoolean())
                    g.getBaseGraph().index().getNodeAutoIndexer().startAutoIndexingProperty("lang");
                g.tx().commit();
            }
        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            if (pick == 1) {
                g.tx().readWrite();
                if (random.nextBoolean())
                    g.getBaseGraph().schema().indexFor(DynamicLabel.label("vertex")).on("name").create();
                if (random.nextBoolean())
                    g.getBaseGraph().schema().indexFor(DynamicLabel.label("vertex")).on("age").create();
                if (random.nextBoolean())
                    g.getBaseGraph().schema().indexFor(DynamicLabel.label("vertex")).on("lang").create();
                g.tx().commit();
            } else if (pick == 2) {
                g.tx().readWrite();
                g.getBaseGraph().index().getNodeAutoIndexer().setEnabled(true);
                if (random.nextBoolean())
                    g.getBaseGraph().index().getNodeAutoIndexer().startAutoIndexingProperty("name");
                if (random.nextBoolean())
                    g.getBaseGraph().index().getNodeAutoIndexer().startAutoIndexingProperty("age");
                if (random.nextBoolean())
                    g.getBaseGraph().index().getNodeAutoIndexer().startAutoIndexingProperty("lang");
                g.tx().commit();
            }
        } else {
            // TODO: add CREW work here.
            // TODO: add meta_property indices when meta_property graph is provided
            //throw new RuntimeException("Could not load graph with " + graphData);
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }
}
