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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;
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
public final class AddEdgeStep extends FlatMapStep<Vertex, Edge> {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(TraverserRequirement.OBJECT);

    private final String label;
    private final Object[] keyValues;
    private final List<Vertex> vertices;
    private final Direction direction;

    public AddEdgeStep(final Traversal.Admin traversal, final Direction direction, final String label, final Vertex vertex, final Object... keyValues) {
        this(traversal, direction, label, IteratorUtils.of(vertex), keyValues);
    }

    public AddEdgeStep(final Traversal.Admin traversal, final Direction direction, final String label, final Iterator<Vertex> vertices, final Object... keyValues) {
        super(traversal);
        this.direction = direction;
        this.label = label;
        this.vertices = IteratorUtils.list(vertices);
        this.keyValues = keyValues;
    }

    @Override
    protected Iterator<Edge> flatMap(final Traverser.Admin<Vertex> traverser) {
        return IteratorUtils.map(this.vertices.iterator(), this.direction.equals(Direction.OUT) ?
                vertex -> traverser.get().addEdge(this.label, vertex, this.keyValues) :
                vertex -> vertex.addEdge(this.label, traverser.get(), this.keyValues));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
