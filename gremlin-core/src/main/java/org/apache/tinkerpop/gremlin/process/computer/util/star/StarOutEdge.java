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
public class StarOutEdge extends StarEdge {

    private final StarVertex starVertex;
    private final StarInVertex starInVertex;

    public StarOutEdge(final Object id, final StarVertex starVertex, final String label, final StarInVertex starInVertex) {
        super(id, label);
        this.starVertex = starVertex;
        this.starInVertex = starInVertex;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        if (direction.equals(Direction.OUT))
            return IteratorUtils.of(this.starVertex);
        else if (direction.equals(Direction.IN))
            return IteratorUtils.of(this.starInVertex);
        else
            return IteratorUtils.of(this.starVertex, this.starInVertex);
    }

    @Override
    public Vertex outVertex() {
        return this.starVertex;
    }

    @Override
    public Vertex inVertex() {
        return this.starInVertex;
    }
}
