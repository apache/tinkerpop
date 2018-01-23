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
package org.apache.tinkerpop.gremlin.process.traversal.traverser.util;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;

public class VertexTraverserSet<S> extends TraverserSet<S> {
    // that should be MultiValueMap<Vertex, Traverser.Admin<S>> but 3.2.2 apache collection do not use generics
    private final MultiValueMap vertexIndex = new MultiValueMap();

    @Override
    public void clear() {
        vertexIndex.clear();
        super.clear();
    }

    @Override
    public boolean add(final Traverser.Admin<S> traverser) {
        if (super.add(traverser)) {
            vertexIndex.put(getHostingVertex(traverser.get()), traverser);
            return true;
        } else {
            return false;
        }
    }

    public Collection<Traverser.Admin<S>> get(Vertex v) {
        return vertexIndex.getCollection(v);
    }

    @Override
    public boolean offer(final Traverser.Admin<S> traverser) {
        return this.add(traverser);
    }

    private static Vertex getHostingVertex(final Object object) {
        Object obj = object;
        while (true) {
            if (obj instanceof Vertex)
                return (Vertex) obj;
            else if (obj instanceof Edge)
                return ((Edge) obj).outVertex();
            else if (obj instanceof Property)
                obj = ((Property) obj).element();
            else
                throw new IllegalStateException("The host of the object is unknown: " + obj.toString() + ':' + obj.getClass().getCanonicalName());
        }
    }
}
