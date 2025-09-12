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
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.structure.Edge;

public class AddEdgeStepPlaceholder<S> extends AbstractAddEdgeStepPlaceholder<S> {

    public AddEdgeStepPlaceholder(final Traversal.Admin traversal, final String edgeLabel) {
        super(traversal, edgeLabel == null ? Edge.DEFAULT_LABEL : edgeLabel);
    }

    public AddEdgeStepPlaceholder(final Traversal.Admin traversal, final GValue<String> edgeLabel) {
        super(traversal, edgeLabel == null || edgeLabel.isNull() ? GValue.of(Edge.DEFAULT_LABEL) : edgeLabel);
    }

    public AddEdgeStepPlaceholder(final Traversal.Admin traversal, final Traversal.Admin<S,String> edgeLabelTraversal) {
        super(traversal, edgeLabelTraversal == null ?
                new ConstantTraversal<>(Edge.DEFAULT_LABEL) : edgeLabelTraversal);
    }

    @Override
    public AddEdgeStep<S> asConcreteStep() {
        AddEdgeStep<S> step = new AddEdgeStep<>(traversal, label instanceof GValueConstantTraversal ? ((GValueConstantTraversal<S, String>) label).getConstantTraversal() : label);
        super.configureConcreteStep(step);
        if (from != null) {
            step.addFrom(from instanceof GValueConstantTraversal ? ((GValueConstantTraversal<S, String>) from).getConstantTraversal() : from);
        }
        if (to != null) {
            step.addTo(to instanceof GValueConstantTraversal ? ((GValueConstantTraversal<S, String>) to).getConstantTraversal() : to);
        }
        return step;
    }

}
