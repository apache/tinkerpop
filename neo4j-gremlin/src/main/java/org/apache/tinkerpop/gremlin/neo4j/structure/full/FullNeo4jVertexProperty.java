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

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jHelper;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertexProperty;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.tinkerpop.api.Neo4jDirection;
import org.neo4j.tinkerpop.api.Neo4jGraphAPI;
import org.neo4j.tinkerpop.api.Neo4jNode;
import org.neo4j.tinkerpop.api.Neo4jRelationship;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FullNeo4jVertexProperty<V> extends Neo4jVertexProperty<V> implements WrappedVertexProperty<Neo4jNode> {

    public static final String VERTEX_PROPERTY_LABEL = "vertexProperty";
    public static final String VERTEX_PROPERTY_PREFIX = Graph.Hidden.hide("");
    public static final String VERTEX_PROPERTY_TOKEN = Graph.Hidden.hide("vertexProperty");


    private Neo4jNode node;

    public FullNeo4jVertexProperty(final Neo4jVertex vertex, final String key, final V value) {
        super(vertex, key, value);
    }

    public FullNeo4jVertexProperty(final Neo4jVertex vertex, final Neo4jNode node) {
        super(vertex, (String) node.getProperty(T.key.getAccessor()), (V) node.getProperty(T.value.getAccessor()));
        this.node = node;
    }

    @Override
    public Set<String> keys() {
        if (isNode()) {
            this.vertex.graph().tx().readWrite();
            final Set<String> keys = new HashSet<>();
            for (final String key : this.node.getKeys()) {
                if (!Graph.Hidden.isHidden(key))
                    keys.add(key);
            }
            return keys;
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        ElementHelper.validateProperty(key, value);
        this.vertex.graph().tx().readWrite();
        if (isNode()) {
            this.node.setProperty(key, value);
            return new FullNeo4jProperty<>(this, key, value);
        } else {
            this.node = ((WrappedGraph<Neo4jGraphAPI>) this.vertex.graph()).getBaseGraph().createNode(VERTEX_PROPERTY_LABEL, this.label());
            this.node.setProperty(T.key.getAccessor(), this.key);
            this.node.setProperty(T.value.getAccessor(), this.value);
            this.node.setProperty(key, value);
            this.vertex.getBaseVertex().connectTo(this.node, VERTEX_PROPERTY_PREFIX.concat(this.key));
            this.vertex.getBaseVertex().setProperty(this.key, VERTEX_PROPERTY_TOKEN);
            return new FullNeo4jProperty<>(this, key, value);
        }
    }

    @Override
    public <U> Property<U> property(final String key) {
        this.vertex.graph().tx().readWrite();
        try {
            if (isNode() && this.node.hasProperty(key))
                return new FullNeo4jProperty<>(this, key, (U) this.node.getProperty(key));
            else
                return Property.empty();
        } catch (IllegalStateException ex) {
            throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());
        } catch (RuntimeException ex) {
            if (Neo4jHelper.isNotFound(ex))
                throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());
            throw ex;
        }
    }

    @Override
    public void remove() {
        this.vertex.graph().tx().readWrite();
        if (isNode()) {
            this.node.relationships(null).forEach(Neo4jRelationship::delete);
            this.node.delete();
            if (this.vertex.getBaseVertex().degree(Neo4jDirection.OUTGOING, VERTEX_PROPERTY_PREFIX.concat(this.key)) == 0) {
                if (this.vertex.getBaseVertex().hasProperty(this.key))
                    this.vertex.getBaseVertex().removeProperty(this.key);
            }
        } else {
            if (this.vertex.getBaseVertex().degree(Neo4jDirection.OUTGOING, VERTEX_PROPERTY_PREFIX.concat(this.key)) == 0) {
                if (this.vertex.getBaseVertex().hasProperty(this.key))
                    this.vertex.getBaseVertex().removeProperty(this.key);
            }
        }
    }

    protected boolean isNode() {
        return null != this.node;
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        if (!isNode()) return Collections.emptyIterator();
        else {
            this.vertex.graph().tx().readWrite();
            return IteratorUtils.map(IteratorUtils.filter(this.node.getKeys().iterator(), key -> !key.equals(T.key.getAccessor()) && !key.equals(T.value.getAccessor()) && ElementHelper.keyExists(key, propertyKeys)), key -> (Property<U>) new FullNeo4jProperty<>(FullNeo4jVertexProperty.this, key, (V) this.node.getProperty(key)));
        }
    }

    @Override
    public Neo4jNode getBaseVertexProperty() {
        return this.node;
    }
}
