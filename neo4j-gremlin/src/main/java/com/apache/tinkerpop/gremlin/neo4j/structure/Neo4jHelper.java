/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.apache.tinkerpop.gremlin.neo4j.structure;

import com.apache.tinkerpop.gremlin.structure.Direction;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Neo4jHelper {

    private Neo4jHelper() {
    }

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

    public static Label[] makeLabels(final String potentialMultiLabel) {
        final String[] splitLabels = potentialMultiLabel.split(Neo4jVertex.LABEL_DELIMINATOR);
        final Label[] labels = new Label[splitLabels.length];
        for (int i = 0; i < splitLabels.length; i++) {
            labels[i] = DynamicLabel.label(splitLabels[i]);
        }
        return labels;
    }
}
