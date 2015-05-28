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

import org.apache.tinkerpop.gremlin.neo4j.structure.full.FullNeo4jVertexProperty;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.tinkerpop.api.Neo4jNode;
import org.neo4j.tinkerpop.api.Neo4jRelationship;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class Neo4jVertex extends Neo4jElement implements Vertex, WrappedVertex<Neo4jNode> {

    public static final String LABEL_DELIMINATOR = "::";

    public Neo4jVertex(final Neo4jNode node, final Neo4jGraph graph) {
        super(node, graph);
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.getBaseVertex().getId());
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Edge.Exceptions.userSuppliedIdsNotSupported();

        this.graph.tx().readWrite();
        final Neo4jNode node = (Neo4jNode) this.baseElement;
        final Neo4jEdge edge = this.graph.createEdge(node.connectTo(((Neo4jVertex) inVertex).getBaseVertex(), label));
        ElementHelper.attachProperties(edge, keyValues);
        return edge;
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return this.property(VertexProperty.Cardinality.single, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        return this.property(this.graph.features().vertex().getCardinality(key), key, value, keyValues);
    }

    @Override
    public abstract <V> VertexProperty<V> property(final String key);

    @Override
    public abstract <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys);

    @Override
    public Neo4jNode getBaseVertex() {
        return (Neo4jNode) this.baseElement;
    }

    @Override
    public String label() {
        this.graph.tx().readWrite();
        return String.join(LABEL_DELIMINATOR, this.labels());
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        this.graph.tx().readWrite();
        return new Iterator<Vertex>() {
            final Iterator<Neo4jRelationship> relationshipIterator = IteratorUtils.filter(0 == edgeLabels.length ?
                    getBaseVertex().relationships(Neo4jHelper.mapDirection(direction)).iterator() :
                    getBaseVertex().relationships(Neo4jHelper.mapDirection(direction), (edgeLabels)).iterator(), graph.getRelationshipPredicate());

            @Override
            public boolean hasNext() {
                return this.relationshipIterator.hasNext();
            }

            @Override
            public Neo4jVertex next() {
                return graph.createVertex(this.relationshipIterator.next().other(getBaseVertex()));
            }
        };
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        this.graph.tx().readWrite();
        return new Iterator<Edge>() {
            final Iterator<Neo4jRelationship> relationshipIterator = IteratorUtils.filter(0 == edgeLabels.length ?
                    getBaseVertex().relationships(Neo4jHelper.mapDirection(direction)).iterator() :
                    getBaseVertex().relationships(Neo4jHelper.mapDirection(direction), (edgeLabels)).iterator(), graph.getRelationshipPredicate());

            @Override
            public boolean hasNext() {
                return this.relationshipIterator.hasNext();
            }

            @Override
            public Neo4jEdge next() {
                return graph.createEdge(this.relationshipIterator.next());
            }
        };
    }

    /////////////// Neo4jVertex Specific Methods for Multi-Label Support ///////////////
    public Set<String> labels() {
        this.graph.tx().readWrite();
        final Set<String> labels = new TreeSet<>(this.getBaseVertex().labels());
        return Collections.unmodifiableSet(labels);
    }

    public void addLabel(final String label) {
        this.graph.tx().readWrite();
        this.getBaseVertex().addLabel(label);
    }

    public void removeLabel(final String label) {
        this.graph.tx().readWrite();
        this.getBaseVertex().removeLabel(label);
    }
    //////////////////////////////////////////////////////////////////////////////////////

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    protected boolean existsInNeo4j(final String key) {
        try {
            return this.getBaseVertex().hasProperty(key);
        } catch (IllegalStateException ex) {
            // if vertex is removed before/after transaction close
            throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id());
        } catch (RuntimeException ex) {
            // if vertex is removed before/after transaction close
            if (Neo4jHelper.isNotFound(ex))
                throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id());
            throw ex;
        }
    }
}