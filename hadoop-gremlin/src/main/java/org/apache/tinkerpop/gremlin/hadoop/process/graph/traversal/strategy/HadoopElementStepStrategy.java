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
package org.apache.tinkerpop.gremlin.hadoop.process.graph.traversal.strategy;

import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopElement;
import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopElementStepStrategy extends AbstractTraversalStrategy {

    private static final HadoopElementStepStrategy INSTANCE = new HadoopElementStepStrategy();

    private HadoopElementStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.isStandard())
            return;

        final StartStep<Element> startStep = (StartStep<Element>) traversal.getStartStep();
        if (startStep.startAssignableTo(Vertex.class, Edge.class)) {
            final HadoopElement element = ((StartStep<?>) startStep).getStart();
            traversal.removeStep(0);
            traversal.getStartStep().getLabel().ifPresent(label -> {
                final Step identityStep = new IdentityStep(traversal);
                identityStep.setLabel(label);
                traversal.addStep(0, identityStep);
            });
            traversal.addStep(0, new GraphStep<>(traversal, EmptyGraph.instance(), element.getClass(), element.id()));
        }
    }

    public static HadoopElementStepStrategy instance() {
        return INSTANCE;
    }
}
