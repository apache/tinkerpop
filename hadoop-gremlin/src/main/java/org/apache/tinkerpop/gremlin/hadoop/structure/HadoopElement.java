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
package org.apache.tinkerpop.gremlin.hadoop.structure;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class HadoopElement implements Element {

    protected Element baseElement;
    protected HadoopGraph graph;

    protected HadoopElement() {
    }

    protected HadoopElement(final Element baseElement, final HadoopGraph graph) {
        this.baseElement = baseElement;
        this.graph = graph;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public Object id() {
        return this.baseElement.id();
    }

    @Override
    public String label() {
        return this.baseElement.label();
    }

    @Override
    public void remove() {
        if (this.baseElement instanceof Vertex)
            throw Vertex.Exceptions.vertexRemovalNotSupported();
        else
            throw Edge.Exceptions.edgeRemovalNotSupported();
    }


    @Override
    public <V> Property<V> property(final String key) {
        return this.baseElement.property(key);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public boolean equals(final Object object) {
        return this.baseElement.equals(object);
    }

    @Override
    public int hashCode() {
        return this.baseElement.hashCode();
    }

    @Override
    public String toString() {
        return this.baseElement.toString();
    }
}
