package com.tinkerpop.gremlin.neo4j.process.graph.step.map;

import com.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import com.tinkerpop.gremlin.process.PathTraverser;
import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.structure.Vertex;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jCypherIterator<T> implements Iterator<Traverser<T>> {

    private final ResourceIterator<Map<String,Object>> iterator;
    private final Step step;
    private final boolean trackPaths;
    private final Neo4jGraph graph;

    public Neo4jCypherIterator(final Step step, final ResourceIterator<Map<String,Object>> iterator, final Neo4jGraph graph) {
        this.iterator = iterator;
        this.step = step;
        this.trackPaths = true;
        this.graph = graph;
    }

    public Neo4jCypherIterator(final ResourceIterator<Map<String,Object>> iterator, final Neo4jGraph graph) {
        this.iterator = iterator;
        this.step = null;
        this.trackPaths = false;
        this.graph = graph;
    }

    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public Traverser<T> next() {
        final Map<String,Object> next = this.iterator.next();
        final Map<String,Object> transformed = next.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                final Object val = entry.getValue();
                if (Node.class.isAssignableFrom(val.getClass())) {
                    return new Neo4jVertex((Node) val, graph);
                } else if (Relationship.class.isAssignableFrom(val.getClass())) {
                    return new Neo4jEdge((Relationship) val, graph);
                } else {
                    return val;
                }
        }));

        return this.trackPaths ?
                new PathTraverser<>(this.step.getAs(), (T) transformed) :
                new SimpleTraverser<>((T) transformed);
    }
}

