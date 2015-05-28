/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.neo4j.structure.full;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jHelper;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertexProperty;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.tinkerpop.api.Neo4jDirection;
import org.neo4j.tinkerpop.api.Neo4jNode;
import org.neo4j.tinkerpop.api.Neo4jRelationship;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class FullNeo4jVertex extends Neo4jVertex {

    public FullNeo4jVertex(final Neo4jNode node, final Neo4jGraph neo4jGraph) {
        super(node, neo4jGraph);
    }



    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.getBaseVertex().getId());
        this.graph.tx().readWrite();
        if (existsInNeo4j(key)) {
            if (this.getBaseVertex().getProperty(key).equals(FullNeo4jVertexProperty.VERTEX_PROPERTY_TOKEN)) {
                if (this.getBaseVertex().degree(Neo4jDirection.OUTGOING, FullNeo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key)) > 1)
                    throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
                else
                    return new FullNeo4jVertexProperty<>(this, this.getBaseVertex().relationships(
                            Neo4jDirection.OUTGOING,
                            FullNeo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key))
                            .iterator().next().end());
            } else {
                return new FullNeo4jVertexProperty<>(this, key, (V) this.getBaseVertex().getProperty(key));
            }
        } else
            return VertexProperty.<V>empty();

    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.getBaseVertex().getId());
        ElementHelper.validateProperty(key, value);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();
        this.graph.tx().readWrite();
        try {
            final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
            if (optionalVertexProperty.isPresent()) return optionalVertexProperty.get();

            final String prefixedKey = FullNeo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key);
            if (this.getBaseVertex().hasProperty(key)) {
                if (this.getBaseVertex().getProperty(key).equals(FullNeo4jVertexProperty.VERTEX_PROPERTY_TOKEN)) {
                    final Neo4jNode node = this.graph.getBaseGraph().createNode(FullNeo4jVertexProperty.VERTEX_PROPERTY_LABEL, key);
                    node.setProperty(T.key.getAccessor(), key);
                    node.setProperty(T.value.getAccessor(), value);
                    this.getBaseVertex().connectTo(node, prefixedKey);
                    final Neo4jVertexProperty<V> property = new FullNeo4jVertexProperty<>(this, node);
                    ElementHelper.attachProperties(property, keyValues); // TODO: make this inlined
                    return property;
                } else {
                    Neo4jNode node = this.graph.getBaseGraph().createNode(FullNeo4jVertexProperty.VERTEX_PROPERTY_LABEL, key);
                    node.setProperty(T.key.getAccessor(), key);
                    node.setProperty(T.value.getAccessor(), this.getBaseVertex().removeProperty(key));
                    this.getBaseVertex().connectTo(node, prefixedKey);
                    this.getBaseVertex().setProperty(key, FullNeo4jVertexProperty.VERTEX_PROPERTY_TOKEN);
                    node = this.graph.getBaseGraph().createNode(FullNeo4jVertexProperty.VERTEX_PROPERTY_LABEL, key);
                    node.setProperty(T.key.getAccessor(), key);
                    node.setProperty(T.value.getAccessor(), value);
                    this.getBaseVertex().connectTo(node, prefixedKey);
                    final Neo4jVertexProperty<V> property = new FullNeo4jVertexProperty<>(this, node);
                    ElementHelper.attachProperties(property, keyValues); // TODO: make this inlined
                    return property;
                }
            } else {
                this.getBaseVertex().setProperty(key, value);
                final Neo4jVertexProperty<V> property = new FullNeo4jVertexProperty<>(this, key, value);
                ElementHelper.attachProperties(property, keyValues); // TODO: make this inlined
                return property;
            }
        } catch (IllegalArgumentException iae) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        }
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        this.graph.tx().readWrite();
        return IteratorUtils.stream(getBaseVertex().getKeys())
                .filter(key -> ElementHelper.keyExists(key, propertyKeys))
                .flatMap(key -> {
                    if (getBaseVertex().getProperty(key).equals(FullNeo4jVertexProperty.VERTEX_PROPERTY_TOKEN))
                        return IteratorUtils.stream(getBaseVertex().relationships(Neo4jDirection.OUTGOING, (FullNeo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key))))
                                .map(relationship -> (VertexProperty<V>) new FullNeo4jVertexProperty(FullNeo4jVertex.this, relationship.end()));
                    else
                        return Stream.of(new FullNeo4jVertexProperty<>(FullNeo4jVertex.this, key, (V) this.getBaseVertex().getProperty(key)));
                }).iterator();
    }

    @Override
    public void remove() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.getBaseVertex().getId());
        this.removed = true;
        this.graph.tx().readWrite();
        try {
            final Neo4jNode node = this.getBaseVertex();
            for (final Neo4jRelationship relationship : node.relationships(Neo4jDirection.BOTH)) {
                final Neo4jNode otherNode = relationship.other(node);
                if (otherNode.hasLabel(FullNeo4jVertexProperty.VERTEX_PROPERTY_LABEL)) {
                    otherNode.relationships(null).forEach(Neo4jRelationship::delete);
                    otherNode.delete(); // meta property node
                } else
                    relationship.delete();
            }
            node.delete();
        } catch (final IllegalStateException ignored) {
            // this one happens if the vertex is still chilling in the tx
        } catch (final RuntimeException ex) {
            if (!Neo4jHelper.isNotFound(ex)) throw ex;
            // this one happens if the vertex is committed
        }
    }
}
