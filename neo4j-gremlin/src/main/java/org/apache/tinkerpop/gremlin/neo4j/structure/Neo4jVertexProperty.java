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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class Neo4jVertexProperty<V> implements VertexProperty<V> {

    protected final Neo4jVertex vertex;
    protected final String key;
    protected final V value;


    public Neo4jVertexProperty(final Neo4jVertex vertex, final String key, final V value) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public Object id() {
        // TODO: Neo4j needs a better ID system for VertexProperties
        return (long) (this.key.hashCode() + this.value.hashCode() + this.vertex.id().hashCode());
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;

    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }


}