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
package org.apache.tinkerpop.gremlin.neo4j.structure.trait;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertexProperty;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.neo4j.tinkerpop.api.Neo4jNode;
import org.neo4j.tinkerpop.api.Neo4jRelationship;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @deprecated As of release 3.3.8, not replaced.
 */
@Deprecated
public interface Neo4jTrait {

    public Predicate<Neo4jNode> getNodePredicate();

    public Predicate<Neo4jRelationship> getRelationshipPredicate();

    public void removeVertex(final Neo4jVertex vertex);

    public <V> VertexProperty<V> getVertexProperty(final Neo4jVertex vertex, final String key);

    public <V> Iterator<VertexProperty<V>> getVertexProperties(final Neo4jVertex vertex, final String... keys);

    public <V> VertexProperty<V> setVertexProperty(final Neo4jVertex vertex, final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues);

    ////

    public boolean supportsMultiProperties();

    public boolean supportsMetaProperties();

    public VertexProperty.Cardinality getCardinality(final String key);

    public void removeVertexProperty(final Neo4jVertexProperty vertexProperty);

    public <V> Property<V> setProperty(final Neo4jVertexProperty vertexProperty, final String key, final V value);

    public <V> Property<V> getProperty(final Neo4jVertexProperty vertexProperty, final String key);

    public <V> Iterator<Property<V>> getProperties(final Neo4jVertexProperty vertexProperty, final String... keys);

    ////

    public Iterator<Vertex> lookupVertices(final Neo4jGraph graph, final List<HasContainer> hasContainers, final Object... ids);

}
