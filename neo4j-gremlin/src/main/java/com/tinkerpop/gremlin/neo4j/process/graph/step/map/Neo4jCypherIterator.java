package com.tinkerpop.gremlin.neo4j.process.graph.step.map;

import com.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jCypherIterator<T> implements Iterator<Map<String, T>> {

    private final ResourceIterator<Map<String, T>> iterator;
    private final Neo4jGraph graph;

    public Neo4jCypherIterator(final ResourceIterator<Map<String, T>> iterator, final Neo4jGraph graph) {
        this.iterator = iterator;
        this.graph = graph;
    }

    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public Map<String, T> next() {
        final Map<String, T> next = this.iterator.next();
        final Map<String, T> transformed = next.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    final T val = entry.getValue();
                    if (Node.class.isAssignableFrom(val.getClass())) {
                        return (T) new Neo4jVertex((Node) val, this.graph);
                    } else if (Relationship.class.isAssignableFrom(val.getClass())) {
                        return (T) new Neo4jEdge((Relationship) val, this.graph);
                    } else {
                        return val;
                    }
                }));

        return transformed;
    }
}

