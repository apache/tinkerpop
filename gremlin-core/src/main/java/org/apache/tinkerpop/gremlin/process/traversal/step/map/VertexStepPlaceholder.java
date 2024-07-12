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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.VertexStepInterface;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Handles the logic of traversing to adjacent vertices or edges given a direction and edge labels for steps like,
 * {@code out}, {@code in}, {@code both}, {@code outE}, {@code inE}, and {@code bothE}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexStepPlaceholder<E extends Element> extends AbstractStep<Vertex, E> implements GValueHolder<Vertex, E>, VertexStepInterface<E> {

    private final GValue<String>[] edgeLabels;
    private Direction direction;
    private final Class<E> returnClass;

    public VertexStepPlaceholder(final Traversal.Admin traversal, final Class<E> returnClass, final Direction direction, final GValue<String>... edgeLabels) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.returnClass = returnClass;
        for (GValue<String> label : edgeLabels) {
            if (label.isVariable()) {
                traversal.getGValueManager().track(label);
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

    public String[] getEdgeLabelsGValueSafe() {
        traversal.getGValueManager().pinGValues(Arrays.asList(edgeLabels));
        return Arrays.stream(GValue.resolveToValues(edgeLabels)).toArray(String[]::new);
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
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        throw new IllegalStateException("VertexStepPlaceholder is not executable");
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.direction, Arrays.asList(this.edgeLabels), this.returnClass.getSimpleName().toLowerCase());
    }

    // todo: why commented out?
//    @Override
//    public int hashCode() {
//        int result = Objects.hash(super.hashCode(), direction, returnClass);
//        // edgeLabel's order does not matter because in("x", "y") and in("y", "x") must be considered equal.
//        if (edgeLabels != null && edgeLabels.length > 0) {
//            final List<String> sortedEdgeLabels = Arrays.stream(edgeLabels)
//                    .sorted(Comparator.nullsLast(Comparator.naturalOrder())).collect(Collectors.toList());
//            for (final String edgeLabel : sortedEdgeLabels) {
//                result = 31 * result + Objects.hashCode(edgeLabel);
//            }
//        }
//        return result;
//    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public Step<Vertex, E> asConcreteStep() {
        return new VertexStep<>(traversal, returnClass, direction, Arrays.stream(GValue.resolveToValues(edgeLabels))
                .map(String.class::cast)
                .toArray(String[]::new));
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
}
