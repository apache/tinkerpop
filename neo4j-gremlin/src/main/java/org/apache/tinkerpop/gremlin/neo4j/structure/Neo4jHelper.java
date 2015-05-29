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
package org.apache.tinkerpop.gremlin.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.neo4j.tinkerpop.api.Neo4jNode;
import org.neo4j.tinkerpop.api.Neo4jRelationship;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Neo4jHelper {

    private Neo4jHelper() {
    }

    public static org.neo4j.tinkerpop.api.Neo4jDirection mapDirection(final Direction direction) {
        if (direction.equals(Direction.OUT))
            return org.neo4j.tinkerpop.api.Neo4jDirection.OUTGOING;
        else if (direction.equals(Direction.IN))
            return org.neo4j.tinkerpop.api.Neo4jDirection.INCOMING;
        else
            return org.neo4j.tinkerpop.api.Neo4jDirection.BOTH;
    }

    public static boolean isDeleted(final Neo4jNode node) {
        try {
            node.getKeys();
            return false;
        } catch (final IllegalStateException e) {
            return true;
        }
    }

    public static boolean isDeleted(final Neo4jRelationship relationship) {
        try {
            relationship.type();
            return false;
        } catch (final IllegalStateException e) {
            return true;
        }
    }

    public static boolean isNotFound(RuntimeException ex) {
        return ex.getClass().getSimpleName().equals("NotFoundException");
    }

    public static boolean keyExistsInNeo4j(final Neo4jNode node, final String key) {
        try {
            return node.hasProperty(key);
        } catch (IllegalStateException ex) {
            // if vertex is removed before/after transaction close
            throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, node.getId());
        } catch (RuntimeException ex) {
            // if vertex is removed before/after transaction close
            if (Neo4jHelper.isNotFound(ex))
                throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, node.getId());
            throw ex;
        }
    }

    public static Neo4jNode getVertexPropertyNode(final Neo4jVertexProperty vertexProperty) {
        return vertexProperty.vertexPropertyNode;
    }

    public static void setVertexPropertyNode(final Neo4jVertexProperty vertexProperty, final Neo4jNode node) {
        vertexProperty.vertexPropertyNode = node;
    }
}