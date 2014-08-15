package com.tinkerpop.gremlin.neo4j.process.graph.step.map;

import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import org.neo4j.graphdb.ResourceIterator;

import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jCypherStartStep<E extends Map<String,Object>> extends FlatMapStep<E, E> implements TraverserSource {

    private final ResourceIterator<Map<String,Object>> cypherResultsIterator;
    private final Neo4jGraph graph;

    public Neo4jCypherStartStep(final ResourceIterator<Map<String,Object>> itty, final Traversal traversal, final Neo4jGraph graph) {
        super(traversal);
        this.cypherResultsIterator = itty;
        this.graph = graph;
    }

    @Override
    public void generateTraverserIterator(final boolean trackPaths) {
        this.starts.clear();
        if (trackPaths)
            this.starts.add(new Neo4jCypherIterator(this, cypherResultsIterator, graph));
        else
            this.starts.add(new Neo4jCypherIterator(cypherResultsIterator, graph));
    }

    protected Traverser<E> processNextStart() {
        final Traverser<E> traverser = this.starts.next();
        traverser.setFuture(this.getNextStep().getAs());
        return traverser;
    }

    public void clear() {
        this.starts.clear();
    }
}
