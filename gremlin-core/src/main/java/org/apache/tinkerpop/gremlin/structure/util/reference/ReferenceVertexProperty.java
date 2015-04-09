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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceVertexProperty<V> extends ReferenceElement<VertexProperty> {

    private ReferenceVertexProperty() {
        super();
    }

    public ReferenceVertexProperty(final VertexProperty vertexProperty) {
        super(vertexProperty);
    }

    @Override
    public VertexProperty<V> attach(final Vertex hostVertex) {
        final Iterator<VertexProperty<V>> vertexPropertyIterator = IteratorUtils.filter(hostVertex.<V>properties(), vp -> vp.id().equals(this.id));
        if (!vertexPropertyIterator.hasNext())
            throw new IllegalStateException("The reference vertex property could not be be found at the provided vertex: " + this);
        return vertexPropertyIterator.next();
    }

    @Override
    public VertexProperty<V> attach(final Graph hostGraph) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "vp*[" + this.id + "]";
    }
}
