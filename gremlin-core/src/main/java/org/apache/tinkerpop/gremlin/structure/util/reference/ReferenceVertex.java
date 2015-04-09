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
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;

import java.util.Collections;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceVertex extends ReferenceElement<Vertex> {

    private ReferenceVertex() {
        super();
    }

    public ReferenceVertex(final Vertex vertex) {
        super(vertex);
    }

    @Override
    public Vertex attach(final Vertex hostVertex) {
        if (hostVertex.id().equals(this.id))
            return hostVertex;
        else
            throw new IllegalStateException("The host vertex must be the reference vertex to attach: " + this + "!=" + hostVertex);
    }

    @Override
    public Vertex attach(final Graph hostGraph) {
        return hostGraph.vertices(this.id).next();
    }

    @Override
    public String toString() {
        return "v*[" + this.id + "]";
    }
}
