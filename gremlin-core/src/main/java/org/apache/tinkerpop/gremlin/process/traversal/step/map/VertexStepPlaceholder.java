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
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles the logic of traversing to adjacent vertices or edges given a direction and edge labels for steps like,
 * {@code out}, {@code in}, {@code both}, {@code outE}, {@code inE}, and {@code bothE}.
 */
public class VertexStepPlaceholder<E extends Element> extends FlatMapStep<Vertex, E> implements GValueHolder<Vertex, E>, VertexStepContract<E> {

    private GValue<String>[] edgeLabels;
    private Direction direction;
    private final Class<E> returnClass;

    public VertexStepPlaceholder(final Traversal.Admin traversal, final Class<E> returnClass, final Direction direction, final GValue<String>... edgeLabels) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.returnClass = returnClass;
        for (GValue<String> label : edgeLabels) {
            if (label.isVariable()) {
                traversal.getGValueManager().register(label);
            }
        }
    }

    @Override
    public Direction getDirection() {
        return this.direction;
    }

    @Override
    public String[] getEdgeLabels() {
        traversal.getGValueManager().pinGValues(Arrays.asList(edgeLabels));
        return Arrays.stream(GValue.resolveToValues(edgeLabels)).toArray(String[]::new);
    }

    @Override
    public GValue<String>[] getEdgeLabelsAsGValues() {
        return edgeLabels;
    }

    @Override
    public Class<E> getReturnClass() {
        return this.returnClass;
    }

    @Override
    public void reverseDirection() {
        this.direction = this.direction.opposite();
    }

    /**
     * Determines if the step returns vertices.
     */
    @Override
    public boolean returnsVertex() {
        return this.returnClass.equals(Vertex.class);
    }

    /**
     * Determines if the step returns edges.
     */
    @Override
    public boolean returnsEdge() {
        return this.returnClass.equals(Edge.class);
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<Vertex> traverser) {
        throw new IllegalStateException("VertexStepPlaceholder is not executable");
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.direction, Arrays.asList(this.edgeLabels), this.returnClass.getSimpleName().toLowerCase());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        VertexStepPlaceholder<?> that = (VertexStepPlaceholder<?>) o;
        return Objects.equals(sortEdgeLabels(edgeLabels), sortEdgeLabels(that.edgeLabels)) &&
                direction == that.direction &&
                Objects.equals(returnClass, that.returnClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sortEdgeLabels(edgeLabels), direction, returnClass);
    }

    @Override
    public VertexStepPlaceholder<E> clone() {
        VertexStepPlaceholder<E> clone = (VertexStepPlaceholder<E>) super.clone();
        clone.direction = this.direction;
        clone.edgeLabels = new GValue[this.edgeLabels.length];
        for (int i = 0; i < this.edgeLabels.length; i++) {
            clone.edgeLabels[i] = this.edgeLabels[i].clone();
        }
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public VertexStep<E> asConcreteStep() {
        VertexStep<E> step = new VertexStep<>(traversal, returnClass, direction, Arrays.stream(GValue.resolveToValues(edgeLabels))
                .map(String.class::cast)
                .toArray(String[]::new));
        TraversalHelper.copyLabels(this, step, false);
        return step;
    }

    @Override
    public boolean isParameterized() {
        return Arrays.stream(edgeLabels).anyMatch(gv -> gv.isVariable());
    }

    @Override
    public void updateVariable(String name, Object value) {
        for (int i = 0; i < edgeLabels.length; i++) {
            if (name.equals(edgeLabels[i].getName())) {
                edgeLabels[i] = GValue.ofString(name, (String) value);
            }
        }
    }

    @Override
    public Collection<GValue<?>> getGValues() {
        return Arrays.asList(edgeLabels);
    }

    @Override
    public void close() throws Exception {
        closeIterator();
    }

    /**
     * Helper method for hashCode() and equals() as edgeLabel's order should not influence equality.
     * in("x", "y") and in("y", "x") must be considered equal.
     */
    private List<GValue<String>> sortEdgeLabels(final GValue<String>[] gValues) {
        if (edgeLabels == null || edgeLabels.length == 0) {
            return List.of();
        }
        final List<GValue<String>> sortedEdgeLabels = Arrays.stream(edgeLabels)
                .sorted(Comparator.nullsLast(new Comparator<GValue<String>>() {
                    @Override
                    public int compare(GValue<String> o1, GValue<String> o2) {
                        // variables come before non-variables
                        if (o1.isVariable() && !o2.isVariable()) {
                            return -1;
                        } else if (!o1.isVariable() && o2.isVariable()) {
                            return 1;
                        } else if (o1.isVariable()) {
                            if (!o2.getName().equals(o1.getName())) {
                                return o1.getName().compareTo(o2.getName()); // compare by variable name
                            } else {
                                return o1.get().compareTo(o2.get()); // use value as tie-breaker
                            }
                        } else {
                            return o1.get().compareTo(o2.get()); // comparison of 2 non-variables
                        }
                    }
                })).collect(Collectors.toList());

        return sortedEdgeLabels;
    }
}
