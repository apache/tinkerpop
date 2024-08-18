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
package org.apache.tinkerpop.gremlin.process.traversal.step.util.structure.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.structure.T;

public class AddEdgeStepGV<S> extends GValueStep<AddEdgeStep, S, S> implements AddEdgeStepStructure {
    private final Parameters parameters = new Parameters();

    public AddEdgeStepGV(final Traversal.Admin traversal, final GValue<String> edgeLabel) {
        super(traversal, new AddEdgeStep(traversal, edgeLabel.get()));
        this.parameters.set(this, T.label, edgeLabel);
    }

    @Override
    public void configure(final Object... keyValues) {
        // all GValues locally stored in the parameters, but resolve them for the concrete step
        this.parameters.set(this, GValue.promoteGValuesInKeyValues(keyValues));
        this.concreteStep.configure(GValue.resolveGValues(keyValues));
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public void addTo(final Traversal.Admin<?, ?> toObject) {
        // pass through to the concrete step because it can't a be a GValue and it gets called from GraphTraversal
        this.concreteStep.addTo(toObject);
    }

    @Override
    public void addFrom(final Traversal.Admin<?, ?> fromObject) {
        // pass through to the concrete step because it can't a be a GValue and it gets called from GraphTraversal
        this.concreteStep.addFrom(fromObject);
    }
}
