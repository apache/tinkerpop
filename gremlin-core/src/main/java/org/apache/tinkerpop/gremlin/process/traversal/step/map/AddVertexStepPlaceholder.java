/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class AddVertexStepPlaceholder<S> extends AbstractAddVertexStepPlaceholder<S> {

    public AddVertexStepPlaceholder(Traversal.Admin traversal, String label) {
        super(traversal, label);
    }

    public AddVertexStepPlaceholder(Traversal.Admin traversal, GValue<String> label) {
        super(traversal, label);
    }

    public AddVertexStepPlaceholder(Traversal.Admin traversal, Traversal.Admin<S, ?> vertexLabelTraversal) {
        super(traversal, vertexLabelTraversal);
    }

    public AddVertexStepPlaceholder(Traversal.Admin traversal, Set<Object> labels) {
        super(traversal, labels);
    }

    public AddVertexStepPlaceholder(Traversal.Admin traversal, List<Traversal.Admin<?, ?>> labelTraversals) {
        super(traversal, labelTraversals);
    }

    @Override
    public AddVertexStep<S> asConcreteStep() {
        AddVertexStep<S> step;
        if (label instanceof List) {
            // Multiple label traversals - each resolved to one String label at execution
            step = new AddVertexStep<>(traversal, (List<Traversal.Admin<?, ?>>) label);
        } else if (label instanceof Set) {
            // Resolve any GValue elements to their string values
            final Set<String> resolvedLabels = new LinkedHashSet<>();
            for (final Object l : (Set<?>) label) {
                if (l instanceof GValue) {
                    resolvedLabels.add((String) ((GValue<?>) l).get());
                } else {
                    resolvedLabels.add((String) l);
                }
            }
            step = new AddVertexStep<>(traversal, resolvedLabels);
        } else if (label instanceof Traversal) {
            step = new AddVertexStep<>(traversal, ((Traversal<S, ?>) label).asAdmin());
        } else if (label instanceof GValue) {
            step = new AddVertexStep<>(traversal, ((GValue<String>) label).get());
        } else {
            step = new AddVertexStep<>(traversal, (String) label);
        }
        super.configureConcreteStep(step);
        return step;
    }
}