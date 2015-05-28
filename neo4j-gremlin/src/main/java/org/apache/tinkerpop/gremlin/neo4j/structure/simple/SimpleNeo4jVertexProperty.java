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

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertexProperty;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class SimpleNeo4jVertexProperty<V> extends Neo4jVertexProperty<V> {

    public SimpleNeo4jVertexProperty(final Neo4jVertex vertex, final String key, final V value) {
        super(vertex, key, value);
    }

    @Override
    public Set<String> keys() {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public <U> Property<U> property(final String key) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public void remove() {
        this.vertex.graph().tx().readWrite();
        if (this.vertex.getBaseVertex().hasProperty(this.key))
            this.vertex.getBaseVertex().removeProperty(this.key);
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }
}
