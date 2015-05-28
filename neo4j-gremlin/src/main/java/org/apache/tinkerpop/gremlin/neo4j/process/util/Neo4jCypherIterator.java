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
package org.apache.tinkerpop.gremlin.neo4j.process.util;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.neo4j.tinkerpop.api.Neo4jNode;
import org.neo4j.tinkerpop.api.Neo4jRelationship;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Neo4jCypherIterator<T> implements Iterator<Map<String, T>> {

    private final Iterator<Map<String, T>> iterator;
    private final Neo4jGraph graph;

    public Neo4jCypherIterator(final Iterator<Map<String, T>> iterator, final Neo4jGraph graph) {
        this.iterator = iterator;
        this.graph = graph;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public Map<String, T> next() {
        return this.iterator.next().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    final T val = entry.getValue();
                    if (Neo4jNode.class.isAssignableFrom(val.getClass())) {
                        return (T) new Neo4jVertex((Neo4jNode) val, this.graph);
                    } else if (Neo4jRelationship.class.isAssignableFrom(val.getClass())) {
                        return (T) new Neo4jEdge((Neo4jRelationship) val, this.graph);
                    } else {
                        return val;
                    }
                }));
    }
}
