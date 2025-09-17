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
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class AddVertexStartStepPlaceholder extends AbstractAddVertexStepPlaceholder<Vertex>
        implements AddVertexStepContract<Vertex>, GValueHolder<Vertex, Vertex> {

    public AddVertexStartStepPlaceholder(final Traversal.Admin traversal, final String label) {
        super(traversal, label == null ? Vertex.DEFAULT_LABEL : label);
    }

    public AddVertexStartStepPlaceholder(final Traversal.Admin traversal, final GValue<String> label) {
        super(traversal, label == null ? GValue.of(Vertex.DEFAULT_LABEL) : label);
    }

    public AddVertexStartStepPlaceholder(final Traversal.Admin traversal, final Traversal.Admin<?,String> vertexLabelTraversal) {
        super(traversal, vertexLabelTraversal == null ?
                new ConstantTraversal<>(Vertex.DEFAULT_LABEL) : (Traversal.Admin<Vertex,String>) vertexLabelTraversal);
    }

    @Override
    public AddVertexStartStep asConcreteStep() {
        AddVertexStartStep step = new AddVertexStartStep(traversal, label instanceof GValueConstantTraversal ? ((GValueConstantTraversal<?, String>) label).getConstantTraversal() : label);
        super.configureConcreteStep(step);
        return step;
    }
}
