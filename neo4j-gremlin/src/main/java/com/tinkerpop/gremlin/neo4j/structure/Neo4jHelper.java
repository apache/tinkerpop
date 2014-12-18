package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Direction;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jHelper {

    public static ExecutionEngine getCypher(final Neo4jGraph graph) {
        return graph.cypher;
    }

    public static org.neo4j.graphdb.Direction mapDirection(final Direction direction) {
        if (direction.equals(Direction.OUT))
            return org.neo4j.graphdb.Direction.OUTGOING;
        else if (direction.equals(Direction.IN))
            return org.neo4j.graphdb.Direction.INCOMING;
        else
            return org.neo4j.graphdb.Direction.BOTH;
    }

    public static RelationshipType[] mapEdgeLabels(final String... edgeLabels) {
        final RelationshipType[] relationshipTypes = new RelationshipType[edgeLabels.length];
        for (int i = 0; i < relationshipTypes.length; i++) {
            relationshipTypes[i] = DynamicRelationshipType.withName(edgeLabels[i]);
        }
        return relationshipTypes;
    }

    public static boolean isDeleted(final Node node) {
        try {
            node.getPropertyKeys();
            return false;
        } catch (final IllegalStateException e) {
            return true;
        }
    }

    public static boolean isDeleted(final Relationship relationship) {
        try {
            relationship.getType();
            return false;
        } catch (final IllegalStateException e) {
            return true;
        }
    }
}
