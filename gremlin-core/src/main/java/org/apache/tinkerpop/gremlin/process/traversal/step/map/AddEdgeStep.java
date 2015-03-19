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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AddEdgeStep extends FlatMapStep<Vertex, Edge> implements Mutating {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(TraverserRequirement.OBJECT);

    private final String edgeLabel;
    private final Object[] keyValues;
    private final List<Vertex> vertices;
    private final Direction direction;

    public AddEdgeStep(final Traversal.Admin traversal, final Direction direction, final String edgeLabel, final Vertex vertex, final Object... keyValues) {
        this(traversal, direction, edgeLabel, IteratorUtils.of(vertex), keyValues);
    }

    public AddEdgeStep(final Traversal.Admin traversal, final Direction direction, final String edgeLabel, final Iterator<Vertex> vertices, final Object... keyValues) {
        super(traversal);
        this.direction = direction;
        this.edgeLabel = edgeLabel;
        this.vertices = IteratorUtils.list(vertices);
        this.keyValues = keyValues;
    }

    public Direction getDirection() {
        return direction;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public Object[] getKeyValues() {
        return keyValues;
    }

    public List<Vertex> getVertices() {
        return vertices;
    }

    @Override
    protected Iterator<Edge> flatMap(final Traverser.Admin<Vertex> traverser) {
        return IteratorUtils.map(this.vertices.iterator(), this.direction.equals(Direction.OUT) ?
                vertex -> traverser.get().addEdge(this.edgeLabel, vertex, this.keyValues) :
                vertex -> vertex.addEdge(this.edgeLabel, traverser.get(), this.keyValues));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
