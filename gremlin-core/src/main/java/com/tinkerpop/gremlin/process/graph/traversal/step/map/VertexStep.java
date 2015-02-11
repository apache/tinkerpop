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
package com.tinkerpop.gremlin.process.graph.traversal.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traversal.step.Reversible;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexStep<E extends Element> extends FlatMapStep<Vertex, E> implements Reversible {

    private final String[] edgeLabels;
    private Direction direction;
    private final Class<E> returnClass;

    public VertexStep(final Traversal.Admin traversal, final Class<E> returnClass, final Direction direction, final String... edgeLabels) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.returnClass = returnClass;
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        return Vertex.class.isAssignableFrom(this.returnClass) ?
                (Iterator<E>) traverser.get().iterators().vertexIterator(this.direction, this.edgeLabels) :
                (Iterator<E>) traverser.get().iterators().edgeIterator(this.direction, this.edgeLabels);
    }

    @Override
    public void reverse() {
        this.direction = this.direction.opposite();
    }

    public Direction getDirection() {
        return this.direction;
    }

    public String[] getEdgeLabels() {
        return this.edgeLabels;
    }

    public Class<E> getReturnClass() {
        return this.returnClass;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction, Arrays.asList(this.edgeLabels), this.returnClass.getSimpleName().toLowerCase());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
