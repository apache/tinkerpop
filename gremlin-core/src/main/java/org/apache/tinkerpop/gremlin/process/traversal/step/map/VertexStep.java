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
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles the logic of traversing to adjacent vertices or edges given a direction and edge labels for steps like,
 * {@code out}, {@code in}, {@code both}, {@code outE}, {@code inE}, and {@code bothE}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexStep<E extends Element> extends FlatMapStep<Vertex, E> implements AutoCloseable, Configuring {

    protected Parameters parameters = new Parameters();
    private final String[] edgeLabels;
    private final GValue<String>[] edgeLabelsGValue;
    private Direction direction;
    private final Class<E> returnClass;

    public VertexStep(final Traversal.Admin traversal, final Class<E> returnClass, final Direction direction, final String... edgeLabels) {
        this(traversal, returnClass, direction, GValue.ensureGValues(edgeLabels));
    }

    public VertexStep(final Traversal.Admin traversal, final Class<E> returnClass, final Direction direction, final GValue<String>... edgeLabels) {
        super(traversal);
        this.direction = direction;
        this.returnClass = returnClass;
        this.edgeLabelsGValue = edgeLabels;

        // convert the GValue<String> to a String[] for the edgeLabels field to cache the values
        this.edgeLabels = Arrays.stream(this.edgeLabelsGValue).map(GValue::get).toArray(String[]::new);
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(null, keyValues);
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        // not passing GValue to graphs at this point. if a graph wants to support GValue, it should implement
        // its own step to do so. in this way, we keep things backwards compatible and don't force folks to have
        // deal with this until they are ready.
        return Vertex.class.isAssignableFrom(this.returnClass) ?
                (Iterator<E>) traverser.get().vertices(this.direction, this.edgeLabels) :
                (Iterator<E>) traverser.get().edges(this.direction, this.edgeLabels);
    }

    public Direction getDirection() {
        return this.direction;
    }

    public String[] getEdgeLabels() {
        return this.edgeLabels;
    }

    public GValue<String>[] getEdgeLabelsGValue() {
        return this.edgeLabelsGValue;
    }

    public Class<E> getReturnClass() {
        return this.returnClass;
    }

    public void reverseDirection() {
        this.direction = this.direction.opposite();
    }

    /**
     * Determines if the step returns vertices.
     */
    public boolean returnsVertex() {
        return this.returnClass.equals(Vertex.class);
    }

    /**
     * Determines if the step returns edges.
     */
    public boolean returnsEdge() {
        return this.returnClass.equals(Edge.class);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.direction, Arrays.asList(this.edgeLabels), this.returnClass.getSimpleName().toLowerCase());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), direction, returnClass);
        // edgeLabel's order does not matter because in("x", "y") and in("y", "x") must be considered equal.
        if (edgeLabels != null && edgeLabels.length > 0) {
            final List<String> sortedEdgeLabels = Arrays.stream(edgeLabels)
                    .sorted(Comparator.nullsLast(Comparator.naturalOrder())).collect(Collectors.toList());
            for (final String edgeLabel : sortedEdgeLabels) {
                result = 31 * result + Objects.hashCode(edgeLabel);
            }
        }
        return result;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public void close() throws Exception {
        closeIterator();
    }
}
