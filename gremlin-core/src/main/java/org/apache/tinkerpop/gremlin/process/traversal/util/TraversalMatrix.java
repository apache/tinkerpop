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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;

import java.util.HashMap;
import java.util.Map;

/**
 * A TraversalMatrix provides random, non-linear access to the steps of a traversal by their step id.
 * This is useful in situations where traversers becomes detached from their traversal (and step) and later need to be re-attached.
 * A classic use case is {@link TraversalVertexProgram} on {@link GraphComputer}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalMatrix<S, E> {

    private final Map<String, Step<?, ?>> matrix = new HashMap<>();
    private final Traversal.Admin<S, E> traversal;

    public TraversalMatrix(final Traversal.Admin<S, E> traversal) {
        this.harvestSteps(this.traversal = traversal);
    }

    public <A, B, C extends Step<A, B>> C getStepById(final String stepId) {
        return (C) this.matrix.get(stepId);
    }

    public Traversal.Admin<S, E> getTraversal() {
        return this.traversal;
    }

    private final void harvestSteps(final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            this.matrix.put(step.getId(), step);
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> globalChild : ((TraversalParent) step).getGlobalChildren()) {
                    this.harvestSteps(globalChild);
                }
                for (final Traversal.Admin<?, ?> localChild : ((TraversalParent) step).getLocalChildren()) {
                    this.harvestSteps(localChild);
                }
            }
        }
    }
}
