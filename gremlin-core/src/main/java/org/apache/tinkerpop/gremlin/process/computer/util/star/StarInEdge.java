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

package org.apache.tinkerpop.gremlin.process.computer.util.star;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StarInEdge extends StarEdge {

    private final StarOutVertex starOutVertex;
    private final StarVertex starVertex;

    public StarInEdge(final Object id, final StarOutVertex starOutVertex, final String label, final StarVertex starVertex) {
        super(id, label);
        this.starOutVertex = starOutVertex;
        this.starVertex = starVertex;

    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        if (direction.equals(Direction.OUT))
            return IteratorUtils.of(this.starOutVertex);
        else if (direction.equals(Direction.IN))
            return IteratorUtils.of(this.starVertex);
        else
            return IteratorUtils.of(this.starOutVertex, this.starVertex);
    }

    @Override
    public Vertex outVertex() {
        return this.starOutVertex;
    }

    @Override
    public Vertex inVertex() {
        return this.starVertex;
    }
}
