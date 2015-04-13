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

package org.apache.tinkerpop.gremlin.structure.util.reference;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceVertexProperty<V> extends ReferenceElement<VertexProperty> implements VertexProperty<V> {

    private ReferenceVertex vertex;

    private ReferenceVertexProperty() {

    }

    public ReferenceVertexProperty(final VertexProperty vertexProperty) {
        super(vertexProperty);
        this.vertex = ReferenceFactory.detach(vertexProperty.element());
    }

    @Override
    public VertexProperty<V> attach(final Vertex hostVertex) {
        if (!hostVertex.equals(this.vertex))
            throw Attachable.Exceptions.canNotAttachVertexPropertyToHostVertex(this, hostVertex);
        final Iterator<VertexProperty<V>> vertexPropertyIterator = IteratorUtils.filter(hostVertex.<V>properties(), vp -> vp.id().equals(this.id));
        if (!vertexPropertyIterator.hasNext())
            throw Attachable.Exceptions.canNotAttachVertexPropertyToHostVertex(this, hostVertex);
        return vertexPropertyIterator.next();
    }

    @Override
    public VertexProperty<V> attach(final Graph hostGraph) {
        return this.attach(this.vertex.attach(hostGraph));
    }

    @Override
    public String toString() {
        return "vp[" + this.id + "]";
    }

    @Override
    public String key() {
        return EMPTY_STRING;
    }

    @Override
    public V value() throws NoSuchElementException {
        return null;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public void remove() {
        throw Property.Exceptions.propertyRemovalNotSupported();
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        return Collections.emptyIterator();
    }
}
