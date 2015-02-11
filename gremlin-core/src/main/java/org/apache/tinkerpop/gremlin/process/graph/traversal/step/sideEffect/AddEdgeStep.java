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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Reversible;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.EnumSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AddEdgeStep extends SideEffectStep<Vertex> implements Reversible {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(
            TraverserRequirement.PATH,
            TraverserRequirement.OBJECT
    );

    // TODO: Weight key based on Traverser.getCount() ?

    private final Direction direction;
    private final String edgeLabel;
    private final String stepLabel;
    private final Object[] propertyKeyValues;

    public AddEdgeStep(final Traversal.Admin traversal, final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        super(traversal);
        this.direction = direction;
        this.edgeLabel = edgeLabel;
        this.stepLabel = stepLabel;
        this.propertyKeyValues = propertyKeyValues;
    }

    @Override
    protected void sideEffect(final Traverser.Admin<Vertex> traverser) {
        final Vertex currentVertex = traverser.get();
        final Vertex otherVertex = traverser.path().get(stepLabel);
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            otherVertex.addEdge(edgeLabel, currentVertex, this.propertyKeyValues);
        }
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            currentVertex.addEdge(edgeLabel, otherVertex, this.propertyKeyValues);
        }
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.direction.name(), this.edgeLabel, this.stepLabel);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
