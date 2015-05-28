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

package org.apache.tinkerpop.gremlin.neo4j.structure.simple;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jHelper;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.tinkerpop.api.Neo4jDirection;
import org.neo4j.tinkerpop.api.Neo4jNode;
import org.neo4j.tinkerpop.api.Neo4jRelationship;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class SimpleNeo4jVertex extends Neo4jVertex {

    public SimpleNeo4jVertex(final Neo4jNode node, final Neo4jGraph neo4jGraph) {
        super(node, neo4jGraph);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.getBaseVertex().getId());
        this.graph.tx().readWrite();
        if (existsInNeo4j(key)) {
            return new SimpleNeo4jVertexProperty<>(this, key, (V) this.getBaseVertex().getProperty(key));
        } else
            return VertexProperty.<V>empty();

    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.getBaseVertex().getId());
        if (cardinality != VertexProperty.Cardinality.single)
            throw VertexProperty.Exceptions.multiPropertiesNotSupported();
        if (keyValues.length > 0)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        ElementHelper.validateProperty(key, value);
        try {
            this.getBaseVertex().setProperty(key, value);
            return new SimpleNeo4jVertexProperty<>(this, key, value);
        } catch (final IllegalArgumentException iae) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        }
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        this.graph.tx().readWrite();
        return (Iterator) IteratorUtils.stream(getBaseVertex().getKeys())
                .filter(key -> ElementHelper.keyExists(key, propertyKeys))
                .map(key -> new SimpleNeo4jVertexProperty<>(SimpleNeo4jVertex.this, key, (V) this.getBaseVertex().getProperty(key))).iterator();
    }

    @Override
    public void remove() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.getBaseVertex().getId());
        this.removed = true;
        this.graph.tx().readWrite();
        try {
            final Neo4jNode node = this.getBaseVertex();
            for (final Neo4jRelationship relationship : node.relationships(Neo4jDirection.BOTH)) {
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
